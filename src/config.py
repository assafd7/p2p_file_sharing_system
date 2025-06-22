import os
from pathlib import Path

# Network settings
DEFAULT_HOST = "0.0.0.0"  # Listen on all interfaces for network connections
DEFAULT_PORT = 8000
BOOTSTRAP_NODES = []

# Directory settings
PROJECT_ROOT = Path(os.path.dirname(os.path.abspath(__file__))).parent
DATA_DIR = PROJECT_ROOT / "data"
TEMP_DIR = DATA_DIR / "temp"
FILES_DIR = DATA_DIR / "files"
CACHE_DIR = DATA_DIR / "cache"
DB_PATH = DATA_DIR / "p2p.db"

# UI settings
WINDOW_TITLE = "P2P File Sharing System"
WINDOW_MIN_WIDTH = 800
WINDOW_MIN_HEIGHT = 600

# Security settings
KEY_SIZE = 2048
TOKEN_EXPIRY_HOURS = 24
CHUNK_SIZE = 1024 * 1024  # 1MB

# DHT Configuration
K_BUCKET_SIZE = 20
ALPHA = 3  # Number of parallel requests
NODE_ID_BITS = 160

# File Management
MAX_FILE_SIZE = 1024 * 1024 * 1024  # 1GB
TEMP_DIR = Path("data/temp")
FILES_DIR = Path("data/files")
CACHE_DIR = Path("data/cache")

# Logging
LOG_DIR = Path("logs")
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_MAX_SIZE = 10 * 1024 * 1024  # 10MB
LOG_BACKUP_COUNT = 5

# Security
ENCRYPTION_KEY_SIZE = 32  # 256 bits
RSA_KEY_SIZE = 2048
HASH_ALGORITHM = "sha256"

# UI
UPDATE_INTERVAL = 1000  # ms

# Create necessary directories
for directory in [TEMP_DIR, FILES_DIR, CACHE_DIR, LOG_DIR]:
    directory.mkdir(parents=True, exist_ok=True) 