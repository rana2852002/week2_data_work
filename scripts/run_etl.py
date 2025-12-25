from pathlib import Path
import sys

# project root
ROOT = Path(__file__).resolve().parents[1]

# add src/ to PYTHONPATH
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from bootcamp_data.etl import make_etl_config, run_etl


def main() -> None:
    cfg = make_etl_config(ROOT)
    run_etl(cfg)


if __name__ == "__main__":
    main()
