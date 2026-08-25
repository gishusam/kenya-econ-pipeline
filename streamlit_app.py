from pathlib import Path
import runpy


dashboard_path = Path(__file__).parent / "dashboard" / "app.py"

runpy.run_path(str(dashboard_path))
