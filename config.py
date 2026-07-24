"""
config.py
Configuration classes and constants for the Haier ESD Monitoring & Dashboard System.

Per the approved architecture (Haier_ESD_Monitoring_Dashboard_Project_Plan.md, §3, §7, §27):
- Environment-based config classes (Development / Production)
- Central location for DB path, poll interval, retention policy, secret keys
"""

import os
from datetime import timedelta

BASE_DIR = os.path.abspath(os.path.dirname(__file__))


class BaseConfig:
    """Shared configuration across all environments."""

    # --- Security ---
    SECRET_KEY = os.environ.get("SECRET_KEY", os.environ.get("ESD_SECRET_KEY", "change-me-in-production"))
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = "Lax"
    PERMANENT_SESSION_LIFETIME = timedelta(hours=12)

    # --- Database Configuration ---
    DATABASE_DIR = os.path.join(BASE_DIR, "database")
    DATABASE_PATH = os.path.join(DATABASE_DIR, "esd_monitoring.db")
    _raw_db_uri = os.environ.get("DATABASE_URL")
    if _raw_db_uri:
        _raw_db_uri = _raw_db_uri.strip()
        if "channel_binding=" in _raw_db_uri:
            _raw_db_uri = _raw_db_uri.split("&channel_binding=")[0].split("?channel_binding=")[0]
        if _raw_db_uri.startswith("postgres://"):
            _raw_db_uri = _raw_db_uri.replace("postgres://", "postgresql://", 1)
            
        if _raw_db_uri.startswith("postgresql://") or _raw_db_uri.startswith("sqlite://"):
            SQLALCHEMY_DATABASE_URI = _raw_db_uri
            SQLALCHEMY_ENGINE_OPTIONS = {}
        else:
            SQLALCHEMY_DATABASE_URI = f"sqlite:///{DATABASE_PATH}"
            SQLALCHEMY_ENGINE_OPTIONS = {
                "connect_args": {"check_same_thread": False, "timeout": 15},
            }
    else:
        SQLALCHEMY_DATABASE_URI = f"sqlite:///{DATABASE_PATH}"
        SQLALCHEMY_ENGINE_OPTIONS = {
            "connect_args": {"check_same_thread": False, "timeout": 15},
        }

    # --- Hardware / Polling (§2, §5, §25-26) ---
    TOTAL_KC868_CHANNELS = 32
    ACTIVE_STATION_COUNT = 20
    RESERVED_EXPANSION_CHANNELS = list(range(21, 29))   # DI 21-28
    SYSTEM_HEALTH_CHANNELS = list(range(29, 33))        # DI 29-32

    KC868_HOST = os.environ.get("KC868_HOST", "192.168.1.50")
    KC868_PORT = int(os.environ.get("KC868_PORT", 502))  # Modbus TCP default
    KC868_UNIT_ID = int(os.environ.get("KC868_UNIT_ID", 1))

    POLL_INTERVAL_MS = int(os.environ.get("POLL_INTERVAL_MS", 750))  # 500-1000ms range
    DEBOUNCE_CONSECUTIVE_POLLS = 3       # stable reads required before logging a change
    HEARTBEAT_INTERVAL_SECONDS = 30
    MAX_CONSECUTIVE_POLL_FAILURES = 3    # -> COMM_LOST watchdog trip
    RECONNECT_BACKOFF_BASE_SECONDS = 2
    RECONNECT_BACKOFF_MAX_SECONDS = 60

    # --- Retention (§5, §28) ---
    RETENTION_DAYS = int(os.environ.get("RETENTION_DAYS", 30))
    RETENTION_ARCHIVE_BEFORE_DELETE = True
    BACKUP_DIR = os.path.join(DATABASE_DIR, "backups")
    BACKUP_RETENTION_DAYS = 30

    # --- Exports (§3, §29) ---
    EXPORT_DIR = os.path.join(BASE_DIR, "exports")
    EXPORT_CSV_DIR = os.path.join(EXPORT_DIR, "csv")
    EXPORT_EXCEL_DIR = os.path.join(EXPORT_DIR, "excel")
    EXPORT_PDF_DIR = os.path.join(EXPORT_DIR, "pdf")

    # --- Logging (§24) ---
    LOG_DIR = os.path.join(BASE_DIR, "logs")
    APP_LOG_FILE = os.path.join(LOG_DIR, "app.log")
    HARDWARE_LOG_FILE = os.path.join(LOG_DIR, "hardware.log")
    ERROR_LOG_FILE = os.path.join(LOG_DIR, "error.log")
    LOG_MAX_BYTES = 10 * 1024 * 1024  # 10MB
    LOG_BACKUP_COUNT = 5

    # --- Real-time push ---
    SSE_RETRY_MS = 2000  # client reconnect hint for Server-Sent Events

    @staticmethod
    def init_directories():
        """Ensure all runtime directories exist."""
        # SQLite DB directories
        # If database URL is SQLite file format, extract folder
        if BaseConfig.SQLALCHEMY_DATABASE_URI.startswith("sqlite:///"):
            db_file_path = BaseConfig.SQLALCHEMY_DATABASE_URI.replace("sqlite:///", "")
            # relative paths will start with no slash, absolute on windows will have drive letter
            db_dir = os.path.dirname(os.path.abspath(db_file_path))
            os.makedirs(db_dir, exist_ok=True)
            
        os.makedirs(BaseConfig.DATABASE_DIR, exist_ok=True)
        os.makedirs(BaseConfig.BACKUP_DIR, exist_ok=True)
        os.makedirs(BaseConfig.EXPORT_CSV_DIR, exist_ok=True)
        os.makedirs(BaseConfig.EXPORT_EXCEL_DIR, exist_ok=True)
        os.makedirs(BaseConfig.EXPORT_PDF_DIR, exist_ok=True)
        os.makedirs(BaseConfig.LOG_DIR, exist_ok=True)


class DevelopmentConfig(BaseConfig):
    DEBUG = True
    ENV = "development"
    KC868_SIMULATED = True  # use simulated driver until real hardware is bench-tested (§35 Phase 1-2)


class ProductionConfig(BaseConfig):
    DEBUG = False
    ENV = "production"
    KC868_SIMULATED = os.environ.get("KC868_SIMULATED", "false").lower() == "true"
    SESSION_COOKIE_SECURE = True


class TestingConfig(BaseConfig):
    TESTING = True
    DEBUG = True
    ENV = "testing"
    SQLALCHEMY_DATABASE_URI = "sqlite:///:memory:"
    KC868_SIMULATED = True


CONFIG_MAP = {
    "development": DevelopmentConfig,
    "production": ProductionConfig,
    "testing": TestingConfig,
}


def get_config():
    """Resolve the active config class from the ESD_ENV environment variable."""
    env_name = os.environ.get("ESD_ENV", "development").lower()
    config_cls = CONFIG_MAP.get(env_name, DevelopmentConfig)
    
    _raw_db_uri = os.environ.get("DATABASE_URL")
    if _raw_db_uri:
        _raw_db_uri = _raw_db_uri.strip()
        if "channel_binding=" in _raw_db_uri:
            _raw_db_uri = _raw_db_uri.split("&channel_binding=")[0].split("?channel_binding=")[0]
        if _raw_db_uri.startswith("postgres://"):
            _raw_db_uri = _raw_db_uri.replace("postgres://", "postgresql://", 1)
            
        if _raw_db_uri.startswith("postgresql://") or _raw_db_uri.startswith("sqlite://"):
            config_cls.SQLALCHEMY_DATABASE_URI = _raw_db_uri
            if _raw_db_uri.startswith("postgresql://"):
                config_cls.SQLALCHEMY_ENGINE_OPTIONS = {
                    "pool_pre_ping": True,
                    "pool_recycle": 300,
                }
            else:
                config_cls.SQLALCHEMY_ENGINE_OPTIONS = {}
        else:
            config_cls.SQLALCHEMY_DATABASE_URI = f"sqlite:///{BaseConfig.DATABASE_PATH}"
            config_cls.SQLALCHEMY_ENGINE_OPTIONS = {
                "connect_args": {"check_same_thread": False, "timeout": 15},
            }
    else:
        config_cls.SQLALCHEMY_DATABASE_URI = f"sqlite:///{BaseConfig.DATABASE_PATH}"
        config_cls.SQLALCHEMY_ENGINE_OPTIONS = {
            "connect_args": {"check_same_thread": False, "timeout": 15},
        }

    config_cls.init_directories()
    return config_cls
