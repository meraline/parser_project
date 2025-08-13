import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from parsers.drom_parser import DromParser
from parsers.drive2_parser import Drive2Parser
