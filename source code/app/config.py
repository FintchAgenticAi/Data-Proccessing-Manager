from pathlib import Path
import os

SOURCE_CODE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = SOURCE_CODE_DIR / "data"
DATABASE_URL = os.getenv("DATABASE_URL", "")
DATABASE_PATH = Path(os.getenv("DATABASE_PATH", str(DATA_DIR / "data_processing_manager.sqlite3")))
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3307"))
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "data_processing_db")
MYSQL_USER = os.getenv("MYSQL_USER", "dulari")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
QUARANTINE_DIR = DATA_DIR / "quarantine"
FILE_MANAGEMENT_URL = "http://127.0.0.1:8001"
AGENTIC_RULE_MANAGER_URL = os.getenv("AGENTIC_RULE_MANAGER_URL", "")
TRANSFORMATION_SERVICE_URL = os.getenv("TRANSFORMATION_SERVICE_URL", "")
VALIDATION_SERVICE_URL = os.getenv("VALIDATION_SERVICE_URL", "")
DEDUPLICATION_SERVICE_URL = os.getenv("DEDUPLICATION_SERVICE_URL", "")
