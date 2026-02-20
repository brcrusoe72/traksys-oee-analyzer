# Contributing

## Setup
1. Create a virtualenv and activate it.
2. Install dependencies:
   ```bash
<<<<<<< HEAD
   pip install -r mes-oee-analyzer/requirements.txt
=======
   pip install -r operations-intelligence-analyzer/requirements.txt
>>>>>>> 7037fd9 (Rebrand to operations-intelligence and restore parser/test compatibility)
   pip install pytest
   ```

## Test before PR
Run full tests:
```bash
<<<<<<< HEAD
python -m pytest mes-oee-analyzer -v
=======
python -m pytest operations-intelligence-analyzer -v
>>>>>>> 7037fd9 (Rebrand to operations-intelligence and restore parser/test compatibility)
```

## Change guidelines
- Keep OEE math production-weighted.
- Add tests for parser compatibility when changing ingest logic.
<<<<<<< HEAD
- Prefer shared constants in `mes-oee-analyzer/shared.py` over duplicate literals.
=======
- Prefer shared constants in `operations-intelligence-analyzer/shared.py` over duplicate literals.
- Keep public naming vendor-neutral; do not introduce vendor trademarks in product names, slugs, metadata, or screenshots unless required for interoperability.
>>>>>>> 7037fd9 (Rebrand to operations-intelligence and restore parser/test compatibility)
