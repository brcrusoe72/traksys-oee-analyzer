# Operations Intelligence Analyzer

Live demo: https://operations-intelligence-analyzer-esh6dt3bptdjg83ubda4wb.streamlit.app/

Production-focused OEE analysis for manufacturing operations. Ingest MES exports and generate Excel/PDF outputs with shift diagnostics, downtime Pareto, and prioritized actions.

Compatibility note: Compatible with MES exports, including common vendor formats.

## Quick start

```bash
pip install -r requirements.txt
python analyze.py your_oee_export.xlsx
streamlit run streamlit_app.py
```

## Core scripts

- `analyze.py`: plant-wide OEE and downtime analysis
- `shift_report.py`: shift deep-dive reporting
- `parse_mes.py`: raw MES parser and format detection
- `parse_passdown.py`: passdown parser
- `oee_history.py`: historical trend/SPC logic

## Input examples

- OEE period detail export
- Event summary export
- Pre-processed DayShiftHour workbook
- Shift passdown sheets
- Optional photo/context attachments

## Internal naming guideline

Keep names vendor-neutral. Do not introduce vendor trademarks into product names, slugs, metadata, or screenshots unless technical compatibility requires it.

## License

MIT
