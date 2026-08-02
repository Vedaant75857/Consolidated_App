import sys
from pathlib import Path

# Add the module2 directory to the path so tests can import `agents.*`.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
