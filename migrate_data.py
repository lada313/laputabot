"""Explicit, non-destructive one-time import of legacy runtime JSON."""
from storage import ROOT, DATA_DIR, prepare_storage, validate_data_dir, read_json, atomic_json, validate_portfolio, validate_tickers


def migrate():
    # Validate the target before any writes, without the legacy-data startup gate.
    validate_data_dir()
    imports = {}
    for name, default in [('portfolio.json', {}), ('history.json', []),
                          ('tickers.json', {}), ('open_trades.json', {}), ('candidates.json', {})]:
        destination = DATA_DIR / name
        if destination.exists():
            continue  # Never overwrite an existing destination.
        data = read_json(ROOT / name, default)
        if name == 'portfolio.json':
            validate_portfolio(data)
        elif name == 'tickers.json':
            validate_tickers(data)
        elif name == 'history.json':
            if not isinstance(data, list):
                raise ValueError('Invalid history')
        elif not isinstance(data, dict):
            raise ValueError('Invalid JSON object')
        imports[destination] = data
    # All source files validated before the first write. Original files stay intact.
    for destination, data in imports.items():
        atomic_json(destination, data)
    prepare_storage()
    print('Runtime data imported. Original files were not modified.')


if __name__ == '__main__':
    migrate()
