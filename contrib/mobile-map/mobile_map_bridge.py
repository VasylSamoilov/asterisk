#!/usr/bin/python3
# SPDX-License-Identifier: GPL-2.0-or-later
"""Bluetooth MAP (BlueZ obexd) <-> chan_mobile SMS bridge.

Receives SMS from a phone over Bluetooth MAP and delivers them to Asterisk
through the AMI action MobileMAPIncoming, so they reach the same 'sms'
extension as SMS received over HFP. Outgoing SMS from chan_mobile
(map_socket=) are pushed to the phone; see mobile_map_outgoing.py.

Adapted from the sms-bridge of the asterisk-iphone-bluetooth-gateway-lab
project by Borisych-C (GPL-2.0-or-later),
https://github.com/Borisych-C/asterisk-iphone-bluetooth-gateway-lab
commit cf286672631c4788eb37b90ad7d8c7242f67a103. The MAP handling,
durable queue and duplicate suppression are theirs and were tested with an
iPhone 16; delivery to Asterisk and configuration were generalised.

Message text is never logged. No writes to the phone's message store.
"""
import argparse
import base64
import datetime as dt
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import signal
import socket
import sqlite3
import threading
import time
import uuid
from zoneinfo import ZoneInfo

LOG = logging.getLogger('mobile-map')
MAX_BODY = 65536
PAGE_SIZE = 100
MAX_QUEUE = 1000
OBEX = 'org.bluez.obex'
IDENTITY_TIME_SLOP = 2  # iPhone notification/listing timestamps differ by one second.
AMI_BODY_CHUNK = 700  # AMI input lines are limited to 1024 bytes.
STATUS_PREFIX = 'outgoing-status:'  # queue ids of status reports about outgoing SMS


def content_identity(sender, stamp, body):
    # Keep the timestamp separate so rounding at a second boundary is tolerated.
    digest = hashlib.sha256(json.dumps([sender.strip(), body], ensure_ascii=False).encode()).hexdigest()
    stamp_s = int(dt.datetime.strptime(stamp, '%Y%m%dT%H%M%S').replace(tzinfo=dt.timezone.utc).timestamp())
    return digest, stamp_s


def fingerprint(device, path, props):
    fields = [device, path.rsplit('/', 1)[-1]]
    fields += [str(props.get(k, '')) for k in ('Timestamp', 'SenderAddress', 'Type', 'Size')]
    return hashlib.sha256(json.dumps(fields, ensure_ascii=False).encode()).hexdigest()


def parse_bmessage(data):
    """Only complete UTF-8 MAP SMS; reject unexpected binary/MMS, never mangle Unicode."""
    if len(data) > MAX_BODY:
        raise ValueError('message too large')
    text = data.decode('utf-8-sig', errors='strict').replace('\r\n', '\n')
    if not text.startswith('BEGIN:BMSG\n') or not text.rstrip().endswith('END:BMSG'):
        raise ValueError('incomplete bMessage')
    header, sep, tail = text.partition('BEGIN:MSG\n')
    if not sep or not re.search(r'^TYPE:SMS_(?:GSM|CDMA)$', header, re.M):
        raise ValueError('unsupported message type')
    if re.search(r'^CHARSET:(?!UTF-8\s*$)', header, re.M | re.I):
        raise ValueError('unsupported charset')
    # The last marker is structural; a literal END:MSG inside SMS is preserved.
    body, sep, footer = tail.rpartition('\nEND:MSG\n')
    if not sep or not re.fullmatch(r'(?:END:BBODY\n)(?:END:BENV\n)+END:BMSG\s*', footer):
        raise ValueError('incomplete body')
    originator = header.split('BEGIN:BENV', 1)[0]
    match = re.search(r'^TEL(?:;[^:\n]*)?:(.*)$', originator, re.M)
    sender = match.group(1).strip() if match else ''
    if not body or '\x00' in body:
        raise ValueError('empty or binary message')
    return sender, body


class Store:
    def __init__(self, path):
        self.db = sqlite3.connect(path, timeout=5)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
          CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS messages (
            id TEXT PRIMARY KEY, sender TEXT, stamp TEXT NOT NULL, body TEXT,
            status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
            due REAL NOT NULL DEFAULT 0, code INTEGER, created REAL NOT NULL,
            accepted REAL, call_id TEXT NOT NULL);
          CREATE INDEX IF NOT EXISTS due_messages ON messages(status,due);
          CREATE TABLE IF NOT EXISTS identities (
            digest TEXT NOT NULL, stamp_s INTEGER NOT NULL, message_id TEXT NOT NULL,
            PRIMARY KEY(digest,stamp_s,message_id));
          CREATE TABLE IF NOT EXISTS aliases (id TEXT PRIMARY KEY, message_id TEXT NOT NULL);
        ''')
        with self.db:
            # Messages older than the first start are never delivered.
            self.db.execute("INSERT OR IGNORE INTO state VALUES ('cutoff',?)", (str(time.time()),))
            self.db.execute("INSERT OR IGNORE INTO state SELECT 'checkpoint',value FROM state WHERE key='cutoff'")

    def value(self, key):
        return float(self.db.execute('SELECT value FROM state WHERE key=?', (key,)).fetchone()[0])

    def checkpoint(self, epoch):
        with self.db:
            self.db.execute("UPDATE state SET value=? WHERE key='checkpoint'", (str(epoch),))

    def seen(self, key):
        return self.db.execute('SELECT 1 FROM messages WHERE id=? UNION ALL SELECT 1 FROM aliases WHERE id=? LIMIT 1', (key, key)).fetchone() is not None

    def remember_identity(self, key, sender, stamp, body):
        digest, stamp_s = content_identity(sender, stamp, body)
        self.db.execute('INSERT OR IGNORE INTO identities VALUES(?,?,?)', (digest, stamp_s, key))

    def enqueue(self, key, sender, stamp, body, blocked=False):
        with self.db:
            if self.seen(key):
                if not blocked and self.db.execute('SELECT 1 FROM messages WHERE id=?', (key,)).fetchone():
                    self.remember_identity(key, sender, stamp, body)
                return False
            if not blocked:
                digest, stamp_s = content_identity(sender, stamp, body)
                match = self.db.execute('SELECT message_id FROM identities WHERE digest=? AND stamp_s BETWEEN ? AND ? LIMIT 1',
                                        (digest, stamp_s - IDENTITY_TIME_SLOP, stamp_s + IDENTITY_TIME_SLOP)).fetchone()
                if match:
                    self.db.execute('INSERT OR IGNORE INTO aliases VALUES(?,?)', (key, match[0]))
                    return False
            count = self.db.execute("SELECT count(*) FROM messages WHERE status!='accepted'").fetchone()[0]
            if count >= MAX_QUEUE:
                raise RuntimeError('queue full; checkpoint preserved')
            self.db.execute('INSERT OR IGNORE INTO messages(id,sender,stamp,body,created,call_id,status) VALUES(?,?,?,?,?,?,?)',
                            (key, sender, stamp, body, time.time(), uuid.uuid4().hex,
                             'blocked' if blocked else 'pending'))
            if not blocked:
                self.remember_identity(key, sender, stamp, body)
            return True

    def next_message(self):
        self.db.row_factory = sqlite3.Row
        return self.db.execute("SELECT * FROM messages WHERE status='pending' ORDER BY due,created LIMIT 1").fetchone()

    def result(self, key, code, attempts):
        """code: 2xx delivered; 0, 408, 429, 5xx retry with backoff; anything else blocked."""
        now = time.time()
        if 200 <= code < 300:
            with self.db:
                # Retain only deduplication/operational metadata after delivery.
                self.db.execute("UPDATE messages SET status='accepted',accepted=?,body=NULL,sender=NULL,code=?,attempts=? WHERE id=?",
                                (now, code, attempts, key))
                self.db.execute("DELETE FROM messages WHERE status='accepted' AND accepted<?", (now - 90 * 86400,))
                self.db.execute('DELETE FROM identities WHERE message_id NOT IN (SELECT id FROM messages)')
                self.db.execute('DELETE FROM aliases WHERE message_id NOT IN (SELECT id FROM messages)')
        elif code == 0 or code in (408, 429) or 500 <= code < 600:
            delay = min(3600, 15 * 2 ** min(attempts - 1, 8))
            with self.db:
                self.db.execute('UPDATE messages SET due=?,code=?,attempts=? WHERE id=?', (now + delay, code, attempts, key))
        else:
            with self.db:
                self.db.execute("UPDATE messages SET status='blocked',code=?,attempts=? WHERE id=?", (code, attempts, key))


def ami_value(value, limit=80):
    """Keep untrusted data out of AMI syntax: printable, no CR/LF, bounded."""
    return ''.join(c for c in str(value) if c.isprintable())[:limit]


def ami_request(config, row, action_id):
    body = row['body'].encode('utf-8')
    if not body or len(body) > MAX_BODY:
        raise ValueError('bad body size')
    encoded = base64.b64encode(body).decode('ascii')
    lines = ['Action: MobileMAPIncoming', 'ActionID: ' + action_id,
             'Device: ' + ami_value(config['asterisk_device']),
             'From: ' + ami_value(row['sender'] or ''),
             'Timestamp: ' + ami_value(row['stamp'])]
    if str(row['id']).startswith(STATUS_PREFIX):
        lines.append('Kind: status')  # SMSVIA=map-status: must not be forwarded as SMS
    lines += ['BodyPart: ' + encoded[i:i + AMI_BODY_CHUNK] for i in range(0, len(encoded), AMI_BODY_CHUNK)]
    return ('\r\n'.join(lines) + '\r\n\r\n').encode('ascii', errors='strict')


def read_block(stream):
    """One AMI response/event: header lines up to a blank line."""
    headers = {}
    total = 0
    while True:
        line = stream.readline(4096)
        if not line:
            raise ConnectionError('AMI closed')
        total += len(line)
        if total > 65536:
            raise ValueError('oversize AMI block')
        line = line.decode('utf-8', errors='replace').rstrip('\r\n')
        if not line:
            if headers:
                return headers
            continue
        key, sep, value = line.partition(':')
        if sep:
            headers.setdefault(key.strip().lower(), value.strip())


def read_response(stream, action_id):
    """Skip events and unrelated blocks; return the response to action_id."""
    for _ in range(100):
        block = read_block(stream)
        if 'response' in block and block.get('actionid') == action_id:
            return block
    raise ValueError('no AMI response')


def response_code(block):
    if block.get('response', '').lower() == 'success':
        return 200
    message = block.get('message', '')
    if message.startswith('Invalid'):
        return 400  # This message can never be accepted: keep it for inspection.
    if 'Permission denied' in message or 'Authentication' in message:
        return 503  # Configuration problem; retry later.
    return 503


def ami_deliver(config, row):
    ami = config['ami']
    with socket.create_connection((ami.get('host', '127.0.0.1'), int(ami.get('port', 5038))), timeout=10) as sock:
        stream = sock.makefile('rb')
        if not stream.readline(256).startswith(b'Asterisk Call Manager'):
            raise ConnectionError('not an AMI server')
        login = 'login-' + uuid.uuid4().hex
        sock.sendall(('Action: Login\r\nActionID: ' + login + '\r\nUsername: ' + ami_value(ami['username']) +
                      '\r\nSecret: ' + ami_value(ami['secret'], 256) + '\r\nEvents: off\r\n\r\n').encode())
        if read_response(stream, login).get('response', '').lower() != 'success':
            LOG.warning('AMI login refused')
            return 503
        action = 'sms-' + row['call_id']
        sock.sendall(ami_request(config, row, action))
        code = response_code(read_response(stream, action))
        try:
            sock.sendall(b'Action: Logoff\r\n\r\n')
        except OSError:
            pass
        return code


class Sender(threading.Thread):
    def __init__(self, config, db_path, wake, stop, deliver=ami_deliver):
        super().__init__(name='ami-deliver', daemon=True)
        self.config, self.db_path, self.wake, self.stop, self.deliver = config, db_path, wake, stop, deliver

    def run(self):
        store = Store(self.db_path)
        while not self.stop.is_set():
            self.wake.clear()
            try:
                row = store.next_message()
                if row is None or row['due'] > time.time():
                    self.wake.wait(3600 if row is None else min(3600, row['due'] - time.time()))
                    continue
                code = 0
                try:
                    code = self.deliver(self.config, row)
                except Exception as exc:
                    # Exception messages can contain private data: log classes only.
                    LOG.warning('delivery retry id=%s error=%s', row['id'][:12], type(exc).__name__)
                store.result(row['id'], code, row['attempts'] + 1)
                LOG.info('delivery id=%s status=%d attempt=%d', row['id'][:12], code, row['attempts'] + 1)
            except Exception as exc:
                LOG.error('queue worker error=%s; preserving pending data', type(exc).__name__)
                self.wake.wait(60)
        store.db.close()


class Receiver:
    def __init__(self, config, state):
        import dbus
        from dbus.mainloop.glib import DBusGMainLoop
        from gi.repository import GLib
        self.dbus, self.GLib = dbus, GLib
        DBusGMainLoop(set_as_default=True)
        self.bus = dbus.SessionBus()
        self.config, self.state = config, state
        self.store = Store(state / 'queue.sqlite3')
        self.zone = ZoneInfo(config['phone_timezone'])
        self.loop = GLib.MainLoop()
        self.session = None
        self.generation = 0
        self.busy = False
        self.rescan = False
        self.stopping = False
        self.retry_timer = 0
        self.scan_timer = 0
        self.transfer_timer = 0
        self.retry_delay = 5
        self.transfer = None
        self.temp_path = state / 'incoming.bmsg'
        self.todo = []
        self.wake, self.stop_worker = threading.Event(), threading.Event()
        self.sender = Sender(config, state / 'queue.sqlite3', self.wake, self.stop_worker)
        self.outgoing = None
        self.bus.add_signal_receiver(self.added, dbus_interface='org.freedesktop.DBus.ObjectManager', signal_name='InterfacesAdded', bus_name=OBEX)
        self.bus.add_signal_receiver(self.removed, dbus_interface='org.freedesktop.DBus.ObjectManager', signal_name='InterfacesRemoved', bus_name=OBEX)
        self.bus.add_signal_receiver(self.changed, dbus_interface='org.freedesktop.DBus.Properties', signal_name='PropertiesChanged', bus_name=OBEX, path_keyword='path')
        self.bus.add_signal_receiver(self.owner_changed, dbus_interface='org.freedesktop.DBus', signal_name='NameOwnerChanged', arg0=OBEX)
        if config.get('submit_socket'):
            from mobile_map_outgoing import Outgoing
            self.outgoing = Outgoing(self)
        GLib.timeout_add_seconds(300, self.reconcile)

    def call(self, path, interface, method, args, success, valid=lambda: True):
        generation = self.generation
        def ok(*result):
            if generation != self.generation or self.stopping or not valid():
                # A CreateSession response after shutdown is released by bus disconnection.
                return
            try:
                success(*result)
            except Exception as exc:
                self.failed(exc)
        def fail(exc):
            if generation == self.generation and not self.stopping and valid():
                self.failed(exc)
        try:
            proxy = self.dbus.Interface(self.bus.get_object(OBEX, path, introspect=False), interface)
            args = [self.dbus.Dictionary(arg, signature='sv') if isinstance(arg, dict) else arg for arg in args]
            getattr(proxy, method)(*args, reply_handler=ok, error_handler=fail, timeout=25)
        except Exception as exc:
            fail(exc)

    def connect(self):
        self.retry_timer = 0
        if self.stopping:
            return False
        self.busy = True
        options = {'Target': 'map'}
        if self.config.get('adapter'):
            options['Source'] = self.config['adapter']
        self.call('/org/bluez/obex', OBEX + '.Client1', 'CreateSession',
                  [self.config['device'], options], self.connected)
        return False

    def connected(self, session):
        self.session = str(session)
        self.call(self.session, OBEX + '.MessageAccess1', 'SetFolder', ['telecom'],
                  lambda: self.call(self.session, OBEX + '.MessageAccess1', 'SetFolder', ['msg'], self.ready))

    def ready(self):
        self.busy = False
        LOG.info('MAP ready; notification subscription managed by BlueZ')
        self.schedule_scan(1)
        if self.outgoing:
            self.outgoing.schedule()

    def clear_timer(self, attr):
        ident = getattr(self, attr)
        if ident:
            self.GLib.source_remove(ident)
            setattr(self, attr, 0)

    def release_session(self):
        old, self.session = self.session, None
        if old:
            try:
                proxy = self.dbus.Interface(self.bus.get_object(OBEX, '/org/bluez/obex', introspect=False), OBEX + '.Client1')
                proxy.RemoveSession(old, reply_handler=lambda: None, error_handler=lambda e: None, timeout=5)
            except Exception:
                pass

    def failed(self, exc):
        if self.stopping:
            return
        self.generation += 1
        if self.outgoing:
            self.outgoing.finish('unknown')
        name = getattr(exc, 'get_dbus_name', lambda: type(exc).__name__)()
        LOG.warning('MAP retry error=%s delay=%ds; checkpoint preserved', name, self.retry_delay)
        self.clear_timer('scan_timer')
        self.clear_timer('transfer_timer')
        self.release_session()
        self.transfer = None
        self.busy = False
        self.todo.clear()
        self.clear_timer('retry_timer')
        self.retry_timer = self.GLib.timeout_add_seconds(self.retry_delay, self.connect)
        self.retry_delay = min(300, self.retry_delay * 2)

    def owner_changed(self, name, old, new):
        if old:
            self.failed(ConnectionError('obex owner changed'))

    def added(self, path, interfaces):
        if self.session and str(path).startswith(self.session + '/'):
            props = interfaces.get(OBEX + '.Message1', {})
            if props.get('Status') == 'notification':
                LOG.info('MAP new message notification')
                self.schedule_scan(2)

    def removed(self, path, interfaces):
        if str(path) == self.session:
            self.failed(ConnectionError('session removed'))

    def changed(self, interface, props, invalidated, path):
        if self.outgoing and str(path) == self.outgoing.transfer and str(interface) == OBEX + '.Transfer1':
            self.outgoing.changed(props)
            return
        if str(path) == self.transfer and str(interface) == OBEX + '.Transfer1':
            self.transfer_status(props)
        elif self.session and str(path).startswith(self.session + '/') and str(interface) == OBEX + '.Message1' and props.get('Status') == 'notification':
            self.schedule_scan(2)

    def reconcile(self):
        if self.session:
            self.schedule_scan(1)
        return True

    def schedule_scan(self, delay):
        if self.stopping:
            return
        if self.busy:
            self.rescan = True
        elif self.session and not self.scan_timer:
            self.scan_timer = self.GLib.timeout_add_seconds(delay, self.scan)

    def scan(self):
        self.scan_timer = 0
        if not self.session or self.busy or self.stopping:
            return False
        self.busy, self.rescan = True, False
        self.scan_started = time.time()
        cutoff = self.store.value('cutoff')
        # Local phone timestamps have second precision; overlap avoids scan races.
        begin = max(cutoff, self.store.value('checkpoint') - 120)
        self.period_begin = dt.datetime.fromtimestamp(begin, self.zone).strftime('%Y%m%dT%H%M%S')
        self.offset = 0
        self.scan_count = 0
        self.scan_queued = 0
        self.todo = []
        self.page()
        return False

    def page(self):
        self.call(self.session, OBEX + '.MessageAccess1', 'ListMessages', ['inbox', {
            'MaxCount': self.dbus.UInt16(PAGE_SIZE), 'Offset': self.dbus.UInt16(self.offset),
            'SubjectLength': self.dbus.Byte(0), 'PeriodBegin': self.period_begin,
            'Types': self.dbus.Array(['sms'], signature='s'),
            'Fields': self.dbus.Array(['timestamp', 'sender-address', 'type', 'size'], signature='s')
        }], self.listed)

    def listed(self, rows):
        self.scan_count += len(rows)
        cutoff = int(self.store.value('cutoff'))
        keys = {item[0] for item in self.todo}
        for path, props in rows.items():
            if str(props.get('Type')) not in ('sms-gsm', 'sms-cdma'):
                continue
            stamp = str(props.get('Timestamp', ''))
            stamp_epoch = dt.datetime.strptime(stamp, '%Y%m%dT%H%M%S').replace(tzinfo=self.zone).timestamp()
            if stamp_epoch < cutoff:
                continue
            if stamp_epoch > time.time() + 300:
                raise ValueError('phone timestamp is in the future')
            key = fingerprint(self.config['device'], str(path), props)
            if not self.store.seen(key) and key not in keys:
                self.todo.append((key, str(path), dict(props)))
                keys.add(key)
        if len(self.todo) > MAX_QUEUE:
            raise RuntimeError('scan backlog too large')
        if len(rows) == PAGE_SIZE:
            self.offset += PAGE_SIZE
            if self.offset > 65000:
                raise RuntimeError('MAP pagination limit')
            self.page()
        else:
            self.todo.sort(key=lambda item: str(item[2]['Timestamp']))
            self.fetch_next()

    def fetch_next(self):
        if not self.todo:
            self.store.checkpoint(self.scan_started)
            if self.scan_count or self.retry_delay != 5:
                LOG.info('MAP reconciliation complete listed=%d queued=%d', self.scan_count, self.scan_queued)
            self.busy = False
            self.retry_delay = 5
            if self.rescan:
                self.schedule_scan(2)
            if self.outgoing:
                self.outgoing.schedule()
            return
        self.current = self.todo.pop(0)
        if int(self.current[2].get('Size', 0)) > MAX_BODY:
            raise ValueError('MAP message too large')
        self.temp_path.unlink(missing_ok=True)
        self.call(self.current[1], OBEX + '.Message1', 'Get', [str(self.temp_path), False], self.get_started)

    def get_started(self, path, props):
        self.transfer = str(path)
        self.transfer_timer = self.GLib.timeout_add_seconds(45, self.transfer_timeout)
        # A transfer can complete before its method reply. Read current status once.
        current_transfer = self.transfer
        self.transfer_status(props)
        if self.transfer == current_transfer:
            self.call(self.transfer, 'org.freedesktop.DBus.Properties', 'GetAll',
                      [OBEX + '.Transfer1'], self.transfer_status,
                      valid=lambda: self.transfer == current_transfer)

    def transfer_timeout(self):
        self.transfer_timer = 0
        self.failed(TimeoutError('MAP transfer timed out'))
        return False

    def transfer_status(self, props):
        if not self.transfer:
            return
        status = str(props.get('Status', ''))
        if status == 'error':
            self.failed(ConnectionError('MAP download failed'))
        elif status == 'complete':
            self.clear_timer('transfer_timer')
            self.transfer = None
            try:
                with self.temp_path.open('rb') as stream:
                    raw = stream.read(MAX_BODY + 1)
                key, path, meta = self.current
                try:
                    sender, body = parse_bmessage(raw)
                except (ValueError, UnicodeError):
                    self.store.enqueue(key, str(meta.get('SenderAddress', '')),
                                       str(meta['Timestamp']), raw, blocked=True)
                    LOG.error('SMS retained for inspection id=%s; unsupported bMessage', key[:12])
                else:
                    sender = str(meta.get('SenderAddress', '')) or sender or 'unknown'
                    if self.store.enqueue(key, sender, str(meta['Timestamp']), body):
                        self.scan_queued += 1
                        LOG.info('SMS durably queued id=%s bytes=%d', key[:12], len(body.encode('utf-8')))
                    else:
                        LOG.info('SMS duplicate suppressed id=%s', key[:12])
                self.temp_path.unlink(missing_ok=True)
                self.wake.set()
                self.fetch_next()
            except Exception as exc:
                self.failed(exc)

    def shutdown(self, *_):
        if self.stopping:
            return False
        self.stopping = True
        if self.outgoing:
            self.outgoing.close()
        self.generation += 1
        self.clear_timer('retry_timer')
        self.clear_timer('scan_timer')
        self.clear_timer('transfer_timer')
        self.release_session()
        self.stop_worker.set()
        self.wake.set()
        self.loop.quit()
        return False

    def run(self):
        for sig in (signal.SIGTERM, signal.SIGINT):
            self.GLib.unix_signal_add(self.GLib.PRIORITY_DEFAULT, sig, self.shutdown)
        self.sender.start()
        self.connect()
        self.loop.run()
        self.sender.join(timeout=50)
        self.store.db.close()


MAC = r'[0-9A-Fa-f]{2}(?::[0-9A-Fa-f]{2}){5}'


def load_config(path):
    config = json.loads(Path(path).read_text())
    if not re.fullmatch(MAC, config.get('device', '')):
        raise ValueError('device must be the phone Bluetooth address')
    if config.get('adapter') and not re.fullmatch(MAC, config['adapter']):
        raise ValueError('adapter must be a Bluetooth address')
    if not re.fullmatch(r'[A-Za-z0-9_.-]+', config.get('asterisk_device', '')):
        raise ValueError('asterisk_device must be the chan_mobile device id')
    ZoneInfo(config['phone_timezone'])  # Required: MAP timestamps are phone-local.
    ami = config.get('ami', {})
    if not ami.get('username') or not ami.get('secret'):
        raise ValueError('ami.username and ami.secret are required')
    return config


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True, type=Path)
    parser.add_argument('--state', required=True, type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    args.state.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(args.state, 0o700)
    config = load_config(args.config)
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    Receiver(config, args.state).run()


if __name__ == '__main__':
    main()
