# laputabot

Private Telegram investment tracker. Buy/sell actions record transactions locally;
they do not submit broker orders.

## Security update: migration before deployment

Stop the existing bot and take an offline copy of its current data first. Use a
single bot process and a persistent private volume. Do not run both versions at
once. Existing JSON files and Git history are deliberately not removed by this PR.

Configure environment variables on the host, using `.env.example` as a reference:

- `TELEGRAM_TOKEN` and `TINKOFF_TOKEN`: hosting secrets. Use a broker read-only token.
- `ALLOWED_USER_IDS`: comma-separated positive numeric Telegram user IDs. Usernames
  are not accepted. Only private chats from these users are authorized.
- `CHAT_ID`: numeric private user ID for notifications, included in the allowlist.
- `DATA_DIR`: absolute path on a persistent private volume, for example `/var/lib/laputabot`.
- `GIT_SYNC=0`: default. Do not set `GIT_TOKEN`.

The code does not automatically load `.env` files. Configure the environment in
Render or the process manager. Never put actual secrets into this repository.

With the same environment as the bot, run:

```sh
python migrate_data.py
python main.py
```

Migration copies the legacy JSON from the application directory to `DATA_DIR`,
validates it, and never overwrites an existing destination or deletes an original.
It can be rerun after interruption. Startup refuses to silently replace unmigrated
legacy data with an empty portfolio. Migration must use the latest data from the
running host, which may differ from the checked-in snapshot.

`ledger.json` becomes authoritative for portfolio and transaction history after
the first trade or reset. The imported `portfolio.json` and `history.json` then
remain migration snapshots. Do not edit them to change current state.

Runtime JSON is replaced atomically, with a `.bak` containing the preceding valid
version. Both files are created with mode 0600 on POSIX. A malformed source or a
failed write must not cause an automatic reset. Restore a verified backup while
the bot is stopped. Back up the volume independently; one local backup does not
protect against loss of the entire host. Protect volume access in hosting settings.

## Access and input limits

Every Telegram update passes an allowlist/private-chat guard before command,
callback and conversation handlers. Missing or malformed access configuration
prevents startup. Updates less than 250 ms apart per authorized user are rejected.

Trade sessions expire after 10 minutes. Price-choice callbacks must refer to the
latest prompt for the active session. Completed trades consume session state;
sales recheck available shares at commit time. Portfolio and history are committed
together before changing memory. Reset needs a fresh confirmation (60 seconds)
and preserves transaction history and the watchlist.

Inputs: ASCII ticker of 1-8 letters/digits; at most 100 watched tickers and portfolio
positions, up to 1,000,000 shares per position; positive finite price/budget up to
1,000,000,000 with at most six decimal places. `/ideas` has a 30-second cooldown and
120-second deadline; manual catalog refresh has 60-second cooldown/deadline.

## Git and previously exposed data

No runtime portfolio, history, watchlist or open trades are synchronized to Git.
Optional legacy catalog synchronization accepts only `Update candidates.json`,
only the exact public catalog path, and only the fixed `lada313/laputabot`
destination. It is disabled by default. Runtime catalog refresh is local; it does
not automatically publish a catalog. HTTPS authentication must use an OS
credential helper; SSH must use an agent. URLs must never contain credentials.
The queue coalesces catalog jobs; Git commands have a 30-second timeout, discard
subprocess output, and never log credentials or command arguments.

The existing root-level JSON files are still tracked and visible in this public
repository. `.gitignore` only prevents adding new untracked runtime files. After
migration, separately remove those files from tracking and decide whether to
rewrite historical commits. Rewriting history does not retract forks or downloads.
Replace the old Git token and inspect hosting logs: the former push error path
printed its credentials. Rotate other secrets if exposure is found. Token rotation
and history changes are not performed by this patch.

## Validation

```sh
python -m unittest discover -s tests -v
python -m compileall -q .
```

Security regression tests run without Telegram/broker credentials and without
network access. Handler tests compile the actual handler bodies with transport
and broker doubles; they do not prove broker SDK compatibility or live trading API
availability. An optional integration test exercises the real Telegram update
pipeline and health endpoint when dependencies are installed.

The legacy pinned `tinkoff-investments==0.2.0b115` could not be resolved from the
package index used during this update. Do not replace the broker SDK with an
unverified namesake package. A full hash-locked dependency set and end-to-end
startup validation require the deployment's trusted broker SDK source. Dependabot
is configured for dependency updates; this patch does not claim a clean CVE scan.
