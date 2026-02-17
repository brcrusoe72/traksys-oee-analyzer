# Contributing

## Setup
1. Create a virtualenv and activate it.
2. Install dependencies:
   ```bash
   pip install -r traksys-oee-analyzer/requirements.txt
   pip install pytest
   ```

## Test before PR
Run full tests:
```bash
python -m pytest traksys-oee-analyzer -v
```

## Change guidelines
- Keep OEE math production-weighted.
- Add tests for parser compatibility when changing ingest logic.
- Prefer shared constants in `traksys-oee-analyzer/shared.py` over duplicate literals.
