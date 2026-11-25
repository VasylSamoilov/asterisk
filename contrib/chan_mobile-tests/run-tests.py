#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-2.0-or-later
"""Build chan_mobile's real SCO functions into socket harnesses and run them.

The functions are extracted mechanically from addons/chan_mobile.c (or the
file given) and compiled against small stand-ins for the Asterisk API, in
an optimized and an ASan/UBSan build. Nothing is written to the source tree.

  sco_harness.c       mbl_read/mbl_write/sco_* framing: 48/60/96/120/240
                      byte blocks, MTU bound, stray and odd blocks, EOF,
                      socket replacement and RX backlog.
  listener_harness.c  do_sco_listen: no idle millisecond polling, EINTR,
                      bounded unload, fatal errors (from Borisych-C).

These do not touch Bluetooth; they cannot replace a test with a phone.
"""
import argparse
from pathlib import Path
import os
import subprocess
import sys
import tempfile

HERE = Path(__file__).resolve().parent
DEFAULT = HERE.parents[1] / 'addons' / 'chan_mobile.c'

SCO_FUNCTIONS = [
    'static int sco_write(int s, char *buf, int len)\n',
    'static void sco_reset_audio(struct mbl_pvt *pvt, int mtu)\n',
    'static void sco_observe_packet(struct mbl_pvt *pvt, int received)\n',
    'static struct ast_frame *mbl_read(struct ast_channel *ast)\n',
    'static int mbl_write(struct ast_channel *ast, struct ast_frame *frame)\n',
]


def function(source, signature):
    start = source.index(signature)
    opening = source.index('{', start)
    depth, end = 1, opening + 1
    while depth:
        depth += (source[end] == '{') - (source[end] == '}')
        end += 1
    return source[start:end] + '\n'


BUILDS = (('optimized', ['-O2']),
          ('asan-ubsan', ['-O1', '-g', '-fsanitize=address,undefined', '-fno-omit-frame-pointer']))


def run(name, harness, include, text, folder, extra=()):
    (folder / include).write_text(text)
    ok = True
    for build, options in BUILDS:
        binary = folder / f'{name}-{build}'
        subprocess.run(['cc', '-std=gnu11', '-Wall', '-Wextra', '-Werror', '-Wno-unused-parameter',
                        '-pthread', *options, *extra, '-I', str(folder), str(HERE / harness), '-o', str(binary)],
                       check=True)
        env = dict(os.environ, ASAN_OPTIONS='detect_leaks=1:abort_on_error=1', UBSAN_OPTIONS='halt_on_error=1')
        done = subprocess.run([str(binary)], capture_output=True, text=True, timeout=30, env=env)
        status = 'PASS' if done.returncode == 0 else 'FAIL'
        ok &= done.returncode == 0
        print(f'{status} {name} ({build})')
        if done.returncode or '-v' in sys.argv:
            sys.stdout.write(done.stdout)
            sys.stdout.write(done.stderr)
    return ok


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('chan_mobile', nargs='?', type=Path, default=DEFAULT)
    parser.add_argument('-v', action='store_true', help='show harness output')
    args = parser.parse_args()
    source = args.chan_mobile.read_text()
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)
        ok = run('sco', 'sco_harness.c', 'sco_functions.inc',
                 ''.join(function(source, sig) for sig in SCO_FUNCTIONS), folder)
        ok &= run('listener', 'listener_harness.c', 'mobile_loop.inc',
                  function(source, 'static void *do_sco_listen(void *data)\n'), folder)
        ok &= map_submit(source, folder)
    return 0 if ok else 1


def map_submit(source, folder):
    """chan_mobile's mbl_map_submit() against the real Python Intake."""
    sys.path.insert(0, str(HERE.parent / 'mobile-map'))
    import mobile_map_outgoing as outgoing
    (folder / 'map_submit.inc').write_text(function(
        source, 'static int mbl_map_submit(const char *device, const char *path, const char *number, const char *body)\n'))
    binary = folder / 'map-submit'
    subprocess.run(['cc', '-std=gnu11', '-Wall', '-Wextra', '-Werror', '-O1', '-g',
                    '-fsanitize=address,undefined', '-I', str(folder), str(HERE / 'map_submit_harness.c'),
                    '-ljansson', '-o', str(binary)], check=True)

    class GLib:
        @staticmethod
        def idle_add(fn, *args):
            fn(*args)

    seen = []

    def submit(request):
        seen.append(request)
        if request.get('to') == 'refuse':
            return dict(ok=False, reason='invalid-number')
        return dict(ok=True, status='queued')

    ok = True
    path = str(folder / 'submit.sock')
    for uid, number, expect in ((os.getuid(), '+15551234567', 0), (os.getuid(), 'refuse', 1), (os.getuid() + 1, '+1555', 1)):
        intake = outgoing.Intake(path, uid, GLib, submit)
        intake.start()
        try:
            body = 'Привет "quoted"\nline2 \\ end'
            done = subprocess.run([str(binary), path, number, body], capture_output=True, text=True, timeout=20)
        finally:
            intake.close()
            intake.join(5)
        good = done.returncode == expect
        if good and expect == 0:
            request = seen[-1]
            good = (request['device'] == 'iphone' and request['to'] == number and request['body'] == body
                    and len(request['id']) == 32 and all(c in '0123456789abcdef' for c in request['id']))
        if good and uid != os.getuid():
            good = not any(r.get('to') == '+1555' for r in seen)
        ok &= good
        print(f"{'PASS' if good else 'FAIL'} map_submit (peer uid {'ok' if uid == os.getuid() else 'wrong'}, {number})")
        if not good or '-v' in sys.argv:
            sys.stdout.write(done.stderr)
    # Nothing listening: refused, no crash.
    done = subprocess.run([str(binary), path, '+1555', 'x'], capture_output=True, text=True, timeout=20)
    good = done.returncode == 1
    ok &= good
    print(f"{'PASS' if good else 'FAIL'} map_submit (bridge not running)")
    return ok


if __name__ == '__main__':
    sys.exit(main())
