"""Repo-root entry point for Streamlit Cloud."""
import sys, os, importlib.util

# Add mes-oee-analyzer to Python path so all modules are importable
_pkg = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mes-oee-analyzer")
sys.path.insert(0, _pkg)

# Load and execute the real app (avoids circular import from same filename)
_spec = importlib.util.spec_from_file_location("_app", os.path.join(_pkg, "streamlit_app.py"))
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
