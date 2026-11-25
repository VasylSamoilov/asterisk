# SPDX-License-Identifier: GPL-2.0-or-later
# Outbox and bMessage tests adapted from asterisk-iphone-bluetooth-gateway-lab
# (Borisych-C, commit cf286672631c4788eb37b90ad7d8c7242f67a103); intake test is new.
import importlib.util
import json
import os
from pathlib import Path
import socket
import sqlite3
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
spec = importlib.util.spec_from_file_location('mobile_map_outgoing', HERE / 'mobile_map_outgoing.py')
o = importlib.util.module_from_spec(spec)
spec.loader.exec_module(o)
N = '+15551234567'


class TestOutgoing(unittest.TestCase):
    def test_number_normalization_and_refusal(self):
        self.assertEqual(o.normalize_number('+1 (555) 123-4567'), N)
        self.assertEqual(o.normalize_number('0015551234567'), N)
        self.assertEqual(o.normalize_number('sms-+15551234567'), N)
        self.assertEqual(o.normalize_number('0501234567', '972'), '+972501234567')
        self.assertEqual(o.normalize_number('0501234567'), '0501234567')  # no country code configured
        self.assertEqual(o.normalize_number('5551234', '1', ''), '5551234')
        for bad in ['', '+', 'abc', '12', '+1555;bad', '1555\r\nX', 'x' * 30, '+' + '1' * 21]:
            with self.assertRaises(ValueError):
                o.normalize_number(bad)

    def test_bmessage_length_unicode_originator_and_stuffing(self):
        data = o.make_bmessage(N, 'שלום Привет\nEND:MSG\nlast')
        self.assertIn(b'TEL:\r\nEND:VCARD\r\nBEGIN:BENV', data)
        head, body = data.split(b'BEGIN:MSG\r\n', 1)
        size = int(head.split(b'LENGTH:')[1].split(b'\r\n')[0])
        self.assertEqual(size, len(b'BEGIN:MSG\r\n' + body.split(b'END:BBODY')[0]))
        self.assertIn(b'\r\n END:MSG\r\n', data)

    def test_crash_never_automatically_resends_and_request_is_idempotent(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'db'
            db = sqlite3.connect(path)
            box = o.Outbox(db)
            self.assertEqual(box.submit('a' * 32, N, 'test'), ('queued', True))
            self.assertEqual(box.submit('a' * 32, N, 'test'), ('queued', False))
            box.mark('a' * 32, 'submitting')
            db.close()
            db = sqlite3.connect(path)
            box = o.Outbox(db)
            self.assertEqual(box.recover(), ['a' * 32])
            self.assertIsNone(box.next())
            self.assertEqual(box.submit('a' * 32, N, 'test'), ('unknown', False))
            self.assertEqual(box.submit('b' * 64, N, 'next'), ('queued', True))
            box.mark('b' * 64, 'handed-to-phone')
            self.assertIsNone(db.execute('select body from outgoing where id=?', ('b' * 64,)).fetchone()[0])
            db.close()

    def test_expired_queued_message_is_never_sent(self):
        db = sqlite3.connect(':memory:')
        box = o.Outbox(db)
        box.submit('a' * 32, N, 'old')
        with db:
            db.execute('UPDATE outgoing SET created=created-90000')
        self.assertEqual(box.expire(), [('a' * 32, N)])
        self.assertIsNone(box.next())
        self.assertEqual(db.execute('SELECT status FROM outgoing').fetchone()[0], 'expired')
        db.close()

    def test_limits_and_blank_text(self):
        db = sqlite3.connect(':memory:')
        box = o.Outbox(db)
        for body in ['', '  ', 'a\x00b', 'я' * 501, None]:
            with self.assertRaises(ValueError):
                box.submit('a' * 32, N, body)
        for key in ['', 'A' * 32, 'a' * 31, 'g' * 32]:
            with self.assertRaises(ValueError):
                box.submit(key, N, 'x')
        db.close()


class ImmediateGLib:
    @staticmethod
    def idle_add(fn, *args):
        fn(*args)


class TestIntake(unittest.TestCase):
    """The socket protocol chan_mobile speaks (mbl_map_submit)."""
    def run_intake(self, uid, request):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / 's.sock')
            seen = []
            def submit(req):
                seen.append(req)
                return dict(ok=True, status='queued')
            intake = o.Intake(path, uid, ImmediateGLib, submit)
            intake.start()
            try:
                with socket.socket(socket.AF_UNIX) as c:
                    c.settimeout(5)
                    c.connect(path)
                    c.sendall(request)
                    reply = json.loads(c.makefile('rb').readline())
            finally:
                intake.close()
                intake.join(5)
            return reply, seen

    def test_authorized_peer_one_line_each_way(self):
        req = dict(id='a' * 32, device='iphone', to=N, body='hi')
        reply, seen = self.run_intake(os.getuid(), json.dumps(req).encode() + b'\n')
        self.assertEqual(reply, dict(ok=True, status='queued'))
        self.assertEqual(seen, [req])

    def test_other_uid_refused(self):
        reply, seen = self.run_intake(os.getuid() + 1, b'{"id":"x"}\n')
        self.assertFalse(reply['ok'])
        self.assertEqual(seen, [])

    def test_garbage_refused(self):
        reply, seen = self.run_intake(os.getuid(), b'[1,2]\n')
        self.assertFalse(reply['ok'])
        self.assertEqual(seen, [])


class FakeGLib:
    def __init__(self):
        self.timers = {}
        self.next = 1
    def timeout_add_seconds(self, delay, fn):
        self.next += 1
        self.timers[self.next] = fn
        return self.next
    def source_remove(self, ident):
        self.timers.pop(ident, None)
    @staticmethod
    def idle_add(fn, *args):
        fn(*args)


class FakeDbus:
    Boolean = bool


class FakeReceiver:
    """The attributes of mobile_map_bridge.Receiver that Outgoing uses."""
    def __init__(self, tmp, notices):
        spec = importlib.util.spec_from_file_location('mobile_map_bridge', HERE / 'mobile_map_bridge.py')
        bridge = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(bridge)
        self.bridge = bridge
        self.config = dict(asterisk_device='iphone', submit_socket=str(Path(tmp) / 's.sock'),
                           submit_uid=os.getuid(), status_notices=notices, default_country_code='44')
        self.state = Path(tmp)
        self.store = bridge.Store(Path(tmp) / 'db')
        self.GLib, self.dbus = FakeGLib(), FakeDbus
        self.session, self.busy, self.scan_timer, self.stopping, self.rescan = '/session1', False, 0, False, False
        self.calls = []
        self.wake = type('E', (), {'set': lambda self: None})()
    def call(self, path, interface, method, args, success, valid=lambda: True):
        self.calls.append((path, interface, method, args, success))
    def schedule_scan(self, delay):
        pass


class TestOutgoingFlow(unittest.TestCase):
    def flow(self, notices, transfer_status):
        with tempfile.TemporaryDirectory() as tmp:
            r = FakeReceiver(tmp, notices)
            out = o.Outgoing(r)
            try:
                self.assertEqual(out.submit(dict(id='c' * 32, device='iphone', to='07700 900123', body='hi')),
                                 dict(ok=True, status='queued'))
                self.assertEqual(out.submit(dict(id='d' * 32, device='other', to='1', body='x'))['reason'], 'wrong-device')
                out.pump()
                path, interface, method, args, success = r.calls[-1]
                self.assertEqual(method, 'PushMessage')
                self.assertIn(b'TEL:+447700900123', Path(args[0]).read_bytes())
                self.assertEqual(r.store.db.execute('SELECT status FROM outgoing').fetchone()[0], 'submitting')
                self.assertTrue(r.busy)
                success('/transfer1', {'Status': transfer_status})
                self.assertFalse(r.busy)
                status = r.store.db.execute('SELECT status FROM outgoing').fetchone()[0]
                notices = r.store.db.execute("SELECT id FROM messages WHERE id LIKE 'outgoing-status:%'").fetchall()
                return status, notices
            finally:
                out.close()
                out.intake.join(5)
                r.store.db.close()

    def test_push_complete_is_handed_to_phone_without_notice_by_default(self):
        self.assertEqual(self.flow(False, 'complete'), ('handed-to-phone', []))

    def test_push_error_is_unknown_and_reported_when_enabled(self):
        status, notices = self.flow(True, 'error')
        self.assertEqual(status, 'unknown')
        self.assertEqual(len(notices), 1)
        self.assertTrue(notices[0][0].startswith(o.STATUS_PREFIX))


if __name__ == '__main__':
    unittest.main()
