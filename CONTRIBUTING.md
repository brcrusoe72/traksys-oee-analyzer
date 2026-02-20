# Contributing

## Setup

1. Create and activate a virtual environment.
2. Install dependencies:
   ```bash
   pip install -r operations-intelligence-analyzer/requirements.txt
   pip install pytest
   ```

## Test Before PR

```bash
python -m pytest operations-intelligence-analyzer -v
```

## Change Guidelines

- Keep OEE math production-weighted.
- Add tests for parser compatibility when changing ingest logic.
- Prefer shared constants in `operations-intelligence-analyzer/shared.py` over duplicate literals.
- Keep public naming vendor-neutral. Do not introduce vendor trademarks in product names, slugs, metadata, or screenshots unless interoperability requires it.
