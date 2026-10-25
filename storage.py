"""Private runtime JSON. Invalid existing files are never replaced on load."""
import json
import math
import os
from pathlib import Path
import shutil
import tempfile
from security import MAX_AMOUNT, MAX_MONEY, MAX_TICKERS, valid_ticker

ROOT = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv('DATA_DIR') or str(ROOT / 'data')).expanduser().resolve()


def data_file(name):
    if Path(name).name != name:
        raise ValueError('Expected a filename')
    return str(DATA_DIR / name)


def validate_data_dir():
    if DATA_DIR.is_relative_to(ROOT) and not DATA_DIR.is_relative_to(ROOT / 'data'):
        raise ValueError('DATA_DIR must be outside the repository or inside its ignored data directory')


def prepare_storage():
    validate_data_dir()
    DATA_DIR.mkdir(parents=True, exist_ok=True, mode=0o700)
    # Never silently start empty when a deployment still has legacy user data.
    for name in ('portfolio.json', 'history.json', 'tickers.json', 'open_trades.json'):
        if name in ('portfolio.json', 'history.json') and (DATA_DIR / 'ledger.json').exists():
            continue
        if not (DATA_DIR / name).exists() and (ROOT / name).exists():
            raise RuntimeError('Legacy data exists. Run python migrate_data.py before starting the bot')
    # Validate existing files before starting any external client/background task.
    ledger = read_json(DATA_DIR / 'ledger.json', None)
    if ledger is not None:
        if not isinstance(ledger, dict) or not isinstance(ledger.get('history'), list):
            raise ValueError('Invalid ledger')
        validate_portfolio(ledger.get('portfolio'))
    for name in ('open_trades.json', 'candidates.json'):
        if not isinstance(read_json(DATA_DIR / name, {}), dict):
            raise ValueError('Invalid runtime JSON object')


def _reject_constant(value):
    raise ValueError('Non-finite JSON number')


def read_json(path, default):
    try:
        with open(path, encoding='utf-8') as stream:
            return json.load(stream, parse_constant=_reject_constant)
    except FileNotFoundError:
        return default


def atomic_json(path, data):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    payload = json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False)
    fd, temporary = tempfile.mkstemp(prefix='.' + target.name, dir=target.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        if target.exists():
            # Verify before backing up; never rotate corrupt data over a good backup.
            read_json(target, None)
            backup_fd, backup_temp = tempfile.mkstemp(prefix='.backup-', dir=target.parent)
            os.close(backup_fd)
            try:
                shutil.copyfile(target, backup_temp)
                os.replace(backup_temp, str(target) + '.bak')
            finally:
                if os.path.exists(backup_temp):
                    os.unlink(backup_temp)
        os.replace(temporary, target)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def validate_portfolio(data):
    if not isinstance(data, dict) or len(data) > MAX_TICKERS:
        raise ValueError('Invalid portfolio structure')
    for ticker, item in data.items():
        if not valid_ticker(ticker) or not isinstance(item, dict):
            raise ValueError('Invalid portfolio position')
        price, amount = item.get('price'), item.get('amount')
        if (isinstance(price, bool) or not isinstance(price, (int, float))
                or not math.isfinite(price) or not 0 < price <= float(MAX_MONEY)
                or isinstance(amount, bool) or not isinstance(amount, int)
                or not 0 < amount <= MAX_AMOUNT):
            raise ValueError('Invalid position price or amount')
    return data


def validate_tickers(data):
    if not isinstance(data, dict) or len(data) > MAX_TICKERS:
        raise ValueError('Invalid watchlist')
    if any(not valid_ticker(t) or not isinstance(n, str) or not 0 < len(n) <= 100 for t, n in data.items()):
        raise ValueError('Invalid ticker or name')
    return data
