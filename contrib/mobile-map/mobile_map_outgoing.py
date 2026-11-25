# SPDX-License-Identifier: GPL-2.0-or-later
"""Authenticated local submission from chan_mobile and a serialized MAP outbox.

chan_mobile (map_socket=) connects to the submit socket, sends one JSON line
{"id", "device", "to", "body"} and reads one JSON line back. The peer uid is
checked with SO_PEERCRED. The message is stored before it is pushed and an
interrupted push is never repeated automatically: a cellular send that may
already have happened must not be sent twice.

Adapted from the sms-bridge of the asterisk-iphone-bluetooth-gateway-lab
project by Borisych-C (GPL-2.0-or-later),
https://github.com/Borisych-C/asterisk-iphone-bluetooth-gateway-lab
commit cf286672631c4788eb37b90ad7d8c7242f67a103.
"""
import datetime as dt
import hashlib
import json
import logging
import os
from pathlib import Path
import pwd
import re
import socket
import struct
import threading
import time

LOG = logging.getLogger('mobile-map')
MAX_OUTGOING = 1000  # bytes of UTF-8; long multipart SMS are not verified over MAP.
REQUEST_ID = r'[a-f0-9]{32,64}'
STATUS_PREFIX = 'outgoing-status:'  # must match mobile_map_bridge.STATUS_PREFIX


def normalize_number(value, default_country_code='', trunk_prefix='0'):
    """Return an address the phone can send to, or raise ValueError."""
    number = str(value).strip()
    if number.startswith('sms-'):  # explicit SMS address, avoids client contact substitution
        number = number[4:]
    number = re.sub(r'[ ().-]', '', number)
    if number.startswith('00'):
        number = '+' + number[2:]
    elif default_country_code and trunk_prefix and number.startswith(trunk_prefix) and not number.startswith('+'):
        number = '+' + default_country_code + number[len(trunk_prefix):]
    if not re.fullmatch(r'\+?[0-9]{3,20}', number) or number in ('+',):
        raise ValueError('invalid-number')
    return number


def make_bmessage(number, body):
    # MAP byte stuffing prevents user text from becoming a structural END marker.
    body = body.replace('\r\n', '\n').replace('\r', '\n')
    body = '\r\n'.join(' ' + line if line.startswith('END:') else line for line in body.split('\n'))
    part = ('BEGIN:MSG\r\n' + body + '\r\nEND:MSG\r\n').encode('utf-8')
    # Empty originator vCard: the phone fills in its own number.
    head = ('BEGIN:BMSG\r\nVERSION:1.0\r\nSTATUS:UNREAD\r\nTYPE:SMS_GSM\r\n'
            'FOLDER:telecom/msg/outbox\r\nBEGIN:VCARD\r\nVERSION:2.1\r\nN:;;;;\r\nTEL:\r\nEND:VCARD\r\n'
            'BEGIN:BENV\r\nBEGIN:VCARD\r\nVERSION:2.1\r\nN:;;;;\r\nTEL:' + number + '\r\nEND:VCARD\r\n'
            'BEGIN:BBODY\r\nCHARSET:UTF-8\r\nLENGTH:' + str(len(part)) + '\r\n').encode()
    return head + part + b'END:BBODY\r\nEND:BENV\r\nEND:BMSG\r\n'


class Outbox:
    """Statuses: queued -> submitting -> handed-to-phone | unknown; queued -> expired."""
    def __init__(self, db):
        self.db = db
        db.executescript('''CREATE TABLE IF NOT EXISTS outgoing (
          id TEXT PRIMARY KEY, number TEXT NOT NULL, body TEXT, status TEXT NOT NULL,
          created REAL NOT NULL, updated REAL NOT NULL);
          CREATE INDEX IF NOT EXISTS outgoing_status ON outgoing(status,created);''')

    def recover(self):
        rows = self.db.execute("SELECT id FROM outgoing WHERE status='submitting'").fetchall()
        with self.db:
            self.db.execute("UPDATE outgoing SET status='unknown',updated=? WHERE status='submitting'", (time.time(),))
        return [row[0] for row in rows]

    def submit(self, key, number, body):
        if not re.fullmatch(REQUEST_ID, key):
            raise ValueError('invalid-request-id')
        if not isinstance(body, str) or not body.strip() or '\x00' in body or len(body.encode()) > MAX_OUTGOING:
            raise ValueError('empty-or-oversize-text')
        with self.db:
            row = self.db.execute('SELECT status FROM outgoing WHERE id=?', (key,)).fetchone()
            if row:
                return row[0], False
            if self.db.execute("SELECT count(*) FROM outgoing WHERE status='queued'").fetchone()[0] >= 100:
                raise ValueError('outbox-full')
            if self.db.execute('SELECT count(*) FROM outgoing WHERE created>?', (time.time() - 3600,)).fetchone()[0] >= 100:
                raise ValueError('hourly-limit')
            self.db.execute("INSERT INTO outgoing VALUES(?,?,?,'queued',?,?)", (key, number, body, time.time(), time.time()))
        return 'queued', True

    def next(self):
        return self.db.execute("SELECT id,number,body FROM outgoing WHERE status='queued' ORDER BY created LIMIT 1").fetchone()

    def expire(self):
        cutoff = time.time() - 86400
        rows = self.db.execute("SELECT id,number FROM outgoing WHERE status='queued' AND created<?", (cutoff,)).fetchall()
        with self.db:
            self.db.execute("UPDATE outgoing SET status='expired',updated=? WHERE status='queued' AND created<?", (time.time(), cutoff))
            self.db.execute("UPDATE outgoing SET body=NULL WHERE status IN ('expired','unknown') AND updated<?", (time.time()-30*86400,))
            self.db.execute("DELETE FROM outgoing WHERE status IN ('expired','unknown','handed-to-phone') AND updated<?", (time.time()-90*86400,))
        return rows

    def mark(self, key, status):
        with self.db:
            self.db.execute('UPDATE outgoing SET status=?,updated=? WHERE id=?', (status, time.time(), key))
            if status == 'handed-to-phone':
                self.db.execute('UPDATE outgoing SET body=NULL WHERE id=?', (key,))
            self.db.execute("DELETE FROM outgoing WHERE status='handed-to-phone' AND updated<?", (time.time()-90*86400,))


def submit_uid(config):
    if 'submit_uid' in config:
        return int(config['submit_uid'])
    return pwd.getpwnam(config.get('submit_user', 'asterisk')).pw_uid


class Intake(threading.Thread):
    """One blocking local socket worker; all database work stays on the GLib thread."""
    def __init__(self, path, allowed_uid, glib, submit):
        super().__init__(name='sms-intake', daemon=True)
        self.path, self.uid, self.glib, self.submit = path, allowed_uid, glib, submit
        self.stopping = threading.Event()
        self.server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        Path(path).unlink(missing_ok=True)
        self.server.bind(path)
        os.chmod(path, 0o660)
        self.server.listen(4)

    def dispatch(self, request, result, done):
        try:
            result.update(self.submit(request))
        except Exception:
            result.update(ok=False, reason='internal-error')
        finally:
            done.set()
        return False

    def run(self):
        while not self.stopping.is_set():
            try:
                conn, _ = self.server.accept()
            except OSError:
                break
            with conn:
                conn.settimeout(3)
                try:
                    _, uid, _ = struct.unpack('3i', conn.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
                    if uid != self.uid:
                        raise ValueError('peer-not-authorized')
                    raw = b''
                    while b'\n' not in raw:
                        part = conn.recv(4096)
                        if not part or len(raw) + len(part) > 8192:
                            raise ValueError('bad-request-size')
                        raw += part
                    request = json.loads(raw.split(b'\n', 1)[0])
                    if not isinstance(request, dict):
                        raise ValueError('invalid-request')
                    result, done = {}, threading.Event()
                    self.glib.idle_add(self.dispatch, request, result, done)
                    if not done.wait(5):
                        result = dict(ok=False, reason='response-timeout')
                    conn.sendall(json.dumps(result).encode() + b'\n')
                except (OSError, ValueError):
                    try:
                        conn.sendall(b'{"ok":false,"reason":"invalid-local-request"}\n')
                    except OSError:
                        pass

    def close(self):
        self.stopping.set()
        try:
            self.server.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.server.close()
        Path(self.path).unlink(missing_ok=True)


class Outgoing:
    def __init__(self, receiver):
        self.r = receiver
        self.config = receiver.config
        self.box = Outbox(receiver.store.db)
        self.current = None
        self.transfer = None
        self.timer = 0
        self.pump_timer = 0
        self.file = receiver.state / 'outgoing.bmsg'
        self.intake = Intake(self.config['submit_socket'], submit_uid(self.config), receiver.GLib, self.submit)
        for key in self.box.recover():
            self.notice(key, 'unknown', 'Bridge restarted during a send: the result is unknown. It will not be resent automatically.')
        self.intake.start()
        self.expiry_timer = receiver.GLib.timeout_add_seconds(3600, self.expire)
        self.expire()

    def expire(self):
        for key, number in self.box.expire():
            self.notice(key, 'expired', 'SMS to ' + number + ' was not sent: the phone was not reachable for 24 hours.')
        return True

    def notice(self, key, status, text):
        """Report an outcome as an incoming message (SMSVIA=map-status), if enabled."""
        if not self.config.get('status_notices', False):
            return
        stamp = dt.datetime.now().strftime('%Y%m%dT%H%M%S')
        self.r.store.enqueue(STATUS_PREFIX + key + ':' + status, self.config.get('status_sender', 'MAP'), stamp,
                             text + '\nRequest ' + key[:8])
        self.r.wake.set()

    def submit(self, request):
        key = str(request.get('id', ''))
        try:
            device = request.get('device')
            if device is not None and device != self.config['asterisk_device']:
                raise ValueError('wrong-device')
            if request.get('check_only') is True:
                if not re.fullmatch(REQUEST_ID, key):
                    raise ValueError('invalid-request-id')
                LOG.info('outgoing check id=%s status=check-only', key[:8])
                return dict(ok=True, status='check-only')
            number = normalize_number(request.get('to', ''), self.config.get('default_country_code', ''),
                                      self.config.get('trunk_prefix', '0'))
            status, fresh = self.box.submit(key, number, request.get('body'))
            if fresh:
                LOG.info('outgoing queued id=%s', key[:8])
            self.schedule()
            return dict(ok=True, status=status)
        except ValueError as exc:
            safe_key = key if re.fullmatch(REQUEST_ID, key) else hashlib.sha256(key.encode()).hexdigest()
            LOG.warning('outgoing rejected id=%s reason=%s', safe_key[:8], str(exc))
            return dict(ok=False, reason=str(exc))

    def schedule(self):
        if not self.r.stopping and not self.pump_timer:
            self.pump_timer = self.r.GLib.timeout_add_seconds(1, self.pump)

    def pump(self):
        self.pump_timer = 0
        self.expire()
        if self.r.stopping or self.r.busy or self.r.scan_timer or not self.r.session:
            return False  # Receiver invokes schedule after connect/reconciliation.
        row = self.box.next()
        if not row:
            return False
        self.current = tuple(row)
        key, number, body = self.current
        self.r.busy = True
        try:
            self.file.write_bytes(make_bmessage(number, body))
            self.box.mark(key, 'submitting')  # Durable BEFORE Push; crash must not resend.
            LOG.info('outgoing MAP attempt id=%s', key[:8])
            self.r.call(self.r.session, 'org.bluez.obex.MessageAccess1', 'PushMessage',
                        [str(self.file), 'outbox', {'Transparent': self.r.dbus.Boolean(False),
                         'Retry': self.r.dbus.Boolean(False), 'Charset': 'utf8'}], self.started)
        except Exception:
            self.finish('unknown')
        return False

    def started(self, path, props):
        self.transfer = str(path)
        self.timer = self.r.GLib.timeout_add_seconds(45, self.expired)
        self.changed(props)

    def changed(self, props):
        if self.current and props.get('Status') in ('complete', 'error'):
            self.finish('handed-to-phone' if props['Status'] == 'complete' else 'unknown')

    def expired(self):
        self.timer = 0
        self.finish('unknown')
        return False

    def finish(self, status):
        if not self.current:
            return
        key, number, _ = self.current
        self.current = None
        self.transfer = None
        if self.timer:
            self.r.GLib.source_remove(self.timer)
            self.timer = 0
        self.box.mark(key, status)
        LOG.info('outgoing result id=%s status=%s', key[:8], status)
        # A completed MAP push means the phone accepted it, not that it was delivered.
        text = ('The phone accepted the SMS to ' + number + '. This does not confirm delivery.'
                if status == 'handed-to-phone' else
                'Sending the SMS to ' + number + ' failed or was interrupted (Bluetooth error). It will not be resent automatically.')
        self.notice(key, status, text)
        self.file.unlink(missing_ok=True)
        self.r.busy = False
        if self.r.rescan:
            self.r.schedule_scan(1)
        self.schedule()

    def close(self):
        self.intake.close()
        self.r.GLib.source_remove(self.expiry_timer)
        if self.pump_timer:
            self.r.GLib.source_remove(self.pump_timer)
        # Any submitting entry remains durable and becomes unknown at next startup.
