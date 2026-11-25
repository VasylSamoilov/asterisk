# Bluetooth MAP SMS bridge for chan_mobile

Some phones, notably the iPhone, do not offer SMS over HFP AT commands, so
chan_mobile's built-in SMS (`sms=yes`) cannot work with them. They do offer
the Bluetooth **Message Access Profile (MAP)**. This bridge uses BlueZ
`obexd` as a MAP client and connects it to chan_mobile:

* **Received SMS** reach the dialplan exactly like HFP SMS: extension `sms`
  in the device `context`, with `SMSSRC`, `SMSNAME`, `SMSTXT`, plus
  `SMSVIA=map` and `SMSTIME` (phone-local timestamp). The bridge delivers them
  with the AMI action `MobileMAPIncoming`.
* **Outgoing SMS** sent with `MessageSend(mobile:<device>/<number>)` are
  handed to the bridge when the device has `map_socket=` set, and pushed to
  the phone with MAP `PushMessage`.

Calls keep using HFP; the bridge only handles messages and runs next to
chan_mobile on the same adapter.

Adapted from the sms-bridge of
[asterisk-iphone-bluetooth-gateway-lab](https://github.com/Borisych-C/asterisk-iphone-bluetooth-gateway-lab)
by Borisych-C (GPL-2.0-or-later), where it was used with an iPhone 16. The MAP
handling, queue and duplicate suppression are theirs; delivery into
chan_mobile and the configuration are new and have **not yet been tested with
a phone**.

## Behaviour worth knowing

* **Durable queue.** Received messages are stored (SQLite, `--state` dir)
  before delivery and retried with backoff (up to 1 h) while Asterisk is
  unreachable. Delivered message text is deleted; only metadata for
  duplicate detection is kept, for 90 days.
* **No history import.** Only messages newer than the bridge's first start
  are delivered.
* **Duplicates.** The iPhone may report the same message again after a
  reconnect with a timestamp one second off. Messages with the same sender
  and text within ±2 s are treated as one. Two genuinely identical messages
  from one sender within 2 s will therefore coalesce.
* **Outgoing is never repeated automatically.** A message is marked
  `submitting` *before* the push. If the bridge or Bluetooth fails during the
  push, the result is `unknown` and the message is not resent: it may already
  have been sent over the cellular network.
* **"Handed to phone" is not "delivered".** A completed `PushMessage` only
  means the phone accepted it; one test message later showed *Not Delivered*
  on the phone. With `status_notices` the bridge reports each outcome back as
  an incoming message from `status_sender` (default `MAP`) with
  `SMSVIA=map-status`. **Never forward those as SMS**: each forwarded report
  would produce another report.
* Limits: outgoing text up to 1000 bytes UTF-8 (multipart over MAP is not
  verified), 100 queued, 100 per hour; queued messages expire after 24 h.
* Message text is never logged.

## Phone setup (iPhone)

Pair the phone normally. The first MAP connection may be rejected; open the
device's entry in *Settings → Bluetooth* on the phone and enable **Show
Notifications**, then let the bridge retry. That switch is the message access
permission; HFP calls working does not imply it is granted.

## Installation

Requirements: BlueZ with `obexd`, Python 3.9+, `python3-dbus`, `python3-gi`.

1. **obexd runs on a user session bus**, so run the bridge as a normal user
   (here `mapbridge`) with lingering enabled, not as root or `asterisk`:

       loginctl enable-linger mapbridge

2. **Socket directory** shared with Asterisk, recreated at boot
   (`/etc/tmpfiles.d/mobile-map.conf`):

       d /run/mobile-map 2750 mapbridge asterisk -

   The setgid bit gives the socket group `asterisk`; the bridge also checks
   the connecting uid (`submit_user`, default `asterisk`) with `SO_PEERCRED`.

3. **AMI user** in `manager.conf` (keep `bindaddr=127.0.0.1`):

       [mobile-map]
       secret = change-me
       deny = 0.0.0.0/0.0.0.0
       permit = 127.0.0.1/255.255.255.255
       read = message
       write = message

4. **chan_mobile device** in `chan_mobile.conf`:

       [iphone]
       address=00:11:22:33:44:55
       adapter=blue
       context=incoming-mobile
       sms=no
       map_socket=/run/mobile-map/submit.sock

5. **Bridge**, as `mapbridge`:

       install -d -m 700 ~/.local/lib/mobile-map ~/.config/mobile-map ~/.local/state/mobile-map ~/.config/systemd/user
       install -m 600 mobile_map_bridge.py mobile_map_outgoing.py ~/.local/lib/mobile-map/
       install -m 600 mobile-map.json.sample ~/.config/mobile-map/mobile-map.json   # then edit it
       install -m 600 mobile-map-bridge.service ~/.config/systemd/user/
       systemctl --user daemon-reload
       systemctl --user enable --now mobile-map-bridge.service
       journalctl --user -u mobile-map-bridge -f

## Configuration (`mobile-map.json`)

| Key | Meaning |
| --- | --- |
| `device` | Phone Bluetooth address. |
| `adapter` | Local adapter address (optional). |
| `asterisk_device` | chan_mobile device id (the `[section]` name). |
| `phone_timezone` | Required. MAP timestamps are phone-local, e.g. `Europe/Berlin`. |
| `ami` | `host`, `port`, `username`, `secret` of the AMI user above. |
| `submit_socket` | Socket chan_mobile connects to; same as `map_socket`. Omit to receive only. |
| `submit_user` / `submit_uid` | Who may submit (default user `asterisk`). |
| `default_country_code`, `trunk_prefix` | Optional: rewrite national numbers, e.g. `972` and `0` turn `0501234567` into `+972501234567`. `00…` always becomes `+…`. |
| `status_notices`, `status_sender` | Report outgoing results back as messages with `SMSVIA=map-status` (default off; sender `MAP`). |

## Dialplan

    [incoming-mobile]
    exten => sms,1,Verbose(1,SMS via ${SMSVIA} from ${SMSSRC} at ${SMSTIME})
     same => n,GotoIf($["${SMSVIA}" = "map-status"]?status)
     same => n,Set(MESSAGE(body)=${SMSSRC}: ${MESSAGE(body)})
     same => n,MessageSend(pjsip:alice)
     same => n,Hangup()
     ; bridge reports about outgoing SMS: never send these back out as SMS
     same => n(status),MessageSend(pjsip:alice)

    ; from a SIP client: send to sms-<number>
    exten => _sms-.,1,MessageSend(mobile:iphone/${EXTEN:4})
     same => n,Verbose(1,MAP submit: ${MESSAGE_SEND_STATUS})

`MESSAGE_SEND_STATUS=SUCCESS` means the bridge queued the message, not that
it was sent. If the device context has no `sms` extension, the AMI action
fails and the bridge keeps the message and retries.

## Asterisk side reference

* `mobile show devices` shows `map` in the SMS column, and `mobile show
  device <id>` shows the socket, for devices with `map_socket`.
* `manager show command MobileMAPIncoming` documents the AMI action.
* The received-SMS variables are documented under the `mobile` message
  technology (`core show doc`/online docs, *Mobile_Message_Technology*).

## Tests

    python3 -m unittest discover -s tests

They cover bMessage parsing, the queue and duplicate rules, AMI framing and
responses against a fake AMI server, and the submit socket. They do not
need BlueZ, and they cannot exercise obexd or a phone.
