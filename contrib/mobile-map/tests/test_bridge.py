# SPDX-License-Identifier: GPL-2.0-or-later
# Parsing, queue and dedup tests adapted from asterisk-iphone-bluetooth-gateway-lab
# (Borisych-C, commit cf286672631c4788eb37b90ad7d8c7242f67a103); AMI tests are new.
import base64
import importlib.util
import io
from pathlib import Path
import socket
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
spec = importlib.util.spec_from_file_location('mobile_map_bridge', HERE / 'mobile_map_bridge.py')
b = importlib.util.module_from_spec(spec)
spec.loader.exec_module(b)

CONFIG = dict(asterisk_device='iphone', ami=dict(host='127.0.0.1', port=0, username='map', secret='s3cret'))


def bmsg(body, sender='+15551234567'):
    return ('BEGIN:BMSG\r\nVERSION:1.0\r\nSTATUS:UNREAD\r\nTYPE:SMS_GSM\r\n'
            'FOLDER:telecom/msg/inbox\r\nBEGIN:VCARD\r\nVERSION:2.1\r\n'
            f'TEL:{sender}\r\nEND:VCARD\r\nBEGIN:BENV\r\nBEGIN:BBODY\r\n'
            f'CHARSET:UTF-8\r\nLENGTH:99\r\nBEGIN:MSG\r\n{body}\r\nEND:MSG\r\n'
            'END:BBODY\r\nEND:BENV\r\nEND:BMSG\r\n').encode()


class TestParsing(unittest.TestCase):
    def test_unicode_multiline_and_literal_marker(self):
        sender, body = b.parse_bmessage(bmsg('Привет! שלום!\r\nEND:MSG\r\nlast'))
        self.assertEqual(sender, '+15551234567')
        self.assertEqual(body, 'Привет! שלום!\nEND:MSG\nlast')

    def test_incomplete_or_wrong_encoding_is_not_sent(self):
        for raw in (bmsg('hello')[:-12], bmsg('hello').replace(b'hello', b'\xff'),
                    bmsg('hello').replace(b'TYPE:SMS_GSM', b'TYPE:MMS'), bmsg('a\x00b')):
            with self.assertRaises((ValueError, UnicodeError)):
                b.parse_bmessage(raw)

    def test_handle_reuse_differs_but_session_reconnect_does_not(self):
        p = dict(Timestamp='20261114T201500', SenderAddress='123', Type='sms-gsm', Size=20)
        original = b.fingerprint('device', '/session1/message42', p)
        self.assertEqual(original, b.fingerprint('device', '/session2/message42', p))
        p['Timestamp'] = '20261114T201600'
        self.assertNotEqual(original, b.fingerprint('device', '/session2/message42', p))


class TestQueue(unittest.TestCase):
    def test_durable_reopen_dedup_retry_and_accept(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'queue.sqlite3'
            store = b.Store(path)
            cutoff = store.value('cutoff')
            store.enqueue('key', 'sender', '20261114T202654', 'Привет! שלום!')
            store.enqueue('key', 'sender', '20261114T202654', 'Привет! שלום!')
            row = store.next_message()
            call = row['call_id']
            store.db.close()
            store = b.Store(path)
            self.assertEqual(store.value('cutoff'), cutoff)
            self.assertEqual(store.next_message()['body'], 'Привет! שלום!')
            store.result('key', 503, 1)
            self.assertGreater(store.next_message()['due'], time.time())
            self.assertEqual(store.next_message()['call_id'], call)
            store.result('key', 200, 2)
            self.assertIsNone(store.next_message())
            self.assertTrue(store.seen('key'))
            row = store.db.execute('SELECT * FROM messages').fetchone()
            self.assertEqual(row['status'], 'accepted')
            self.assertIsNone(row['body'])
            self.assertIsNone(row['sender'])
            store.db.close()

    def test_permanent_rejection_preserves_message_and_next_can_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = b.Store(Path(tmp) / 'db')
            store.enqueue('a', 'sender', '20261114T202654', 'do not lose')
            store.result('a', 400, 1)
            self.assertIsNone(store.next_message())
            self.assertEqual(store.db.execute('SELECT body FROM messages').fetchone()[0], 'do not lose')
            store.enqueue('b', 'sender', '20261114T202654', 'next')
            self.assertEqual(store.next_message()['id'], 'b')
            with patch.object(b, 'MAX_QUEUE', 2):
                with self.assertRaises(RuntimeError):
                    store.enqueue('c', 'sender', '20261114T202654', 'full')
            store.db.close()

    def test_iphone_timestamp_shift_and_handle_change_after_accept_and_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'db'
            store = b.Store(path)
            self.assertTrue(store.enqueue('notification-handle', 'FUND', '20261114T202654', 'code 123456'))
            store.result('notification-handle', 200, 1)
            store.db.close()
            store = b.Store(path)
            self.assertFalse(store.enqueue('listing-handle', 'FUND', '20261114T202653', 'code 123456'))
            self.assertTrue(store.seen('listing-handle'))
            self.assertIsNone(store.next_message())
            self.assertEqual(store.db.execute('SELECT count(*) FROM messages').fetchone()[0], 1)
            # Distinct body, sender, or sufficiently later genuine repetition survives.
            self.assertTrue(store.enqueue('new-code', 'FUND', '20261114T202654', 'code 654321'))
            self.assertTrue(store.enqueue('other-sender', 'OTHER', '20261114T202654', 'code 123456'))
            self.assertTrue(store.enqueue('later', 'FUND', '20261114T202657', 'code 123456'))
            store.db.close()

    def test_duplicate_while_original_pending_does_not_create_second_job(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = b.Store(Path(tmp) / 'db')
            store.enqueue('first', 'sender', '20261114T202654', 'same')
            self.assertFalse(store.enqueue('second', 'sender', '20261114T202653', 'same'))
            self.assertEqual(store.db.execute('SELECT count(*) FROM messages').fetchone()[0], 1)
            store.db.close()


def row(body='שלום Привет', sender='+15551234567', key='k1'):
    return dict(id=key, body=body, sender=sender, stamp='20261114T202654', call_id='abcdef1234567890')


def headers(request):
    lines = request.decode().split('\r\n')
    return lines, [line for line in lines if line.startswith('BodyPart: ')]


class TestAmiRequest(unittest.TestCase):
    def test_body_split_into_short_lines_and_round_trips(self):
        body = 'Привет ' * 400 + '\nEND:MSG\nlast'
        lines, parts = headers(b.ami_request(CONFIG, row(body), 'id1'))
        self.assertGreater(len(parts), 1)
        self.assertTrue(all(len(line) < 1024 for line in lines))
        decoded = base64.b64decode(''.join(p[len('BodyPart: '):] for p in parts)).decode()
        self.assertEqual(decoded, body)
        self.assertIn('Device: iphone', lines)
        self.assertTrue(b.ami_request(CONFIG, row(), 'id1').endswith(b'\r\n\r\n'))

    def test_header_injection_from_sender(self):
        lines, _ = headers(b.ami_request(CONFIG, row(sender='evil\r\nAction: Originate\r\n'), 'id1'))
        self.assertNotIn('Action: Originate', lines)
        self.assertEqual(sum(1 for line in lines if line.startswith('Action:')), 1)

    def test_status_reports_are_marked(self):
        lines, _ = headers(b.ami_request(CONFIG, row(), 'id1'))
        self.assertNotIn('Kind: status', lines)
        lines, _ = headers(b.ami_request(CONFIG, row(key=b.STATUS_PREFIX + 'a' * 32 + ':unknown'), 'id1'))
        self.assertIn('Kind: status', lines)

    def test_empty_or_oversize_body_refused(self):
        for body in ('', 'x' * (b.MAX_BODY + 1)):
            with self.assertRaises(ValueError):
                b.ami_request(CONFIG, row(body), 'id1')

    def test_response_matching_skips_events_and_other_actions(self):
        stream = io.BytesIO(b'Event: FullyBooted\r\nStatus: x\r\n\r\n'
                            b'Response: Success\r\nActionID: other\r\n\r\n'
                            b'Response: Error\r\nActionID: mine\r\nMessage: Unknown device\r\n\r\n')
        block = b.read_response(stream, 'mine')
        self.assertEqual(b.response_code(block), 503)
        self.assertEqual(b.response_code({'response': 'Success'}), 200)
        self.assertEqual(b.response_code({'response': 'Error', 'message': 'Invalid: empty or binary body'}), 400)
        with self.assertRaises(ConnectionError):
            b.read_response(io.BytesIO(b''), 'mine')


class FakeAmi(threading.Thread):
    """Minimal AMI server: banner, Login, one action, Logoff."""
    def __init__(self, login_ok=True, answer='Success', message='Message queued'):
        super().__init__(daemon=True)
        self.server = socket.socket()
        self.server.bind(('127.0.0.1', 0))
        self.server.listen(1)
        self.port = self.server.getsockname()[1]
        self.login_ok, self.answer, self.message = login_ok, answer, message
        self.blocks = []

    def run(self):
        conn, _ = self.server.accept()
        with conn, conn.makefile('rb') as stream:
            conn.sendall(b'Asterisk Call Manager/9.0.0\r\n')
            while True:
                try:
                    block = b.read_block(stream)
                except ConnectionError:
                    break
                self.blocks.append(block)
                action, ident = block.get('action', '').lower(), block.get('actionid', '')
                if action == 'login':
                    status = 'Success' if self.login_ok else 'Error'
                    conn.sendall(f'Response: {status}\r\nActionID: {ident}\r\nMessage: x\r\n\r\n'.encode())
                elif action == 'mobilemapincoming':
                    conn.sendall(b'Event: Noise\r\n\r\n' + f'Response: {self.answer}\r\nActionID: {ident}\r\nMessage: {self.message}\r\n\r\n'.encode())
                elif action == 'logoff':
                    break
        self.server.close()


class TestAmiDelivery(unittest.TestCase):
    def deliver(self, server):
        server.start()
        config = dict(CONFIG, ami=dict(CONFIG['ami'], port=server.port))
        code = b.ami_deliver(config, row())
        server.join(5)
        return code

    def test_success(self):
        server = FakeAmi()
        self.assertEqual(self.deliver(server), 200)
        action = [blk for blk in server.blocks if blk.get('action') == 'MobileMAPIncoming'][0]
        self.assertEqual(action['device'], 'iphone')
        self.assertEqual(action['timestamp'], '20261114T202654')

    def test_login_refused_retries(self):
        self.assertEqual(self.deliver(FakeAmi(login_ok=False)), 503)

    def test_invalid_is_permanent_unknown_device_retries(self):
        self.assertEqual(self.deliver(FakeAmi(answer='Error', message='Invalid: empty or binary body')), 400)
        self.assertEqual(self.deliver(FakeAmi(answer='Error', message='Unknown device')), 503)

    def test_sender_thread_marks_accepted(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'db'
            store = b.Store(path)
            store.enqueue('k', '+1555', '20261114T202654', 'hi')
            store.db.close()
            seen, wake, stop = [], threading.Event(), threading.Event()
            def deliver(config, r):
                seen.append(r['body'])
                stop.set()
                return 200
            sender = b.Sender(CONFIG, path, wake, stop, deliver=deliver)
            sender.start()
            sender.join(5)
            store = b.Store(path)
            self.assertEqual(seen, ['hi'])
            self.assertEqual(store.db.execute('SELECT status FROM messages').fetchone()[0], 'accepted')
            store.db.close()


class TestConfig(unittest.TestCase):
    def test_required_fields(self):
        good = dict(device='02:00:00:00:00:01', asterisk_device='iphone', phone_timezone='UTC',
                    ami=dict(username='u', secret='s'))
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'c.json'
            import json
            path.write_text(json.dumps(good))
            self.assertEqual(b.load_config(path)['asterisk_device'], 'iphone')
            for key, value in (('device', 'nope'), ('asterisk_device', 'a/b'), ('phone_timezone', 'Nowhere/X')):
                path.write_text(json.dumps(dict(good, **{key: value})))
                with self.assertRaises(Exception):
                    b.load_config(path)


if __name__ == '__main__':
    unittest.main()
