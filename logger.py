"""
logger.py
Centralized logging module for the Haier ESD Monitoring & Dashboard System.

Provides a single, reusable point of logging configuration so any module in
the project (models.py, database.py, app.py, background jobs, etc.) can
obtain a properly configured logger without duplicating handler setup.

Mirrors the logging conventions already established in app.py::configure_logging
(§24 - rotating file handlers for app / hardware / error logs) but is exposed
as an importable, idempotent module so it can be reused across the codebase
without re-wiring handlers on every call.

Log channels (consistent with config.py file paths):
    "app"      -> general application / request logging   (cfg.APP_LOG_FILE)
    "hardware" -> KC868 driver / poller diagnostics         (cfg.HARDWARE_LOG_FILE)
    "error"    -> root logger error-level capture           (cfg.ERROR_LOG_FILE)

Usage:
    from logger import configure_logging, get_logger

    configure_logging(cfg)          # call once, at application startup
    log = get_logger(__name__)      # anywhere else in the codebase
    log.info("Station %s polled", station_code)
"""

import os
import logging
from logging.handlers import RotatingFileHandler

# Guard flag so repeated calls to configure_logging() (e.g. from tests,
# the CLI, or module re-imports) never attach duplicate handlers to the
# root/app/hardware loggers.
_LOGGING_CONFIGURED = False

# Shared formatter, identical layout to app.py's configure_logging() so log
# files remain consistent regardless of which module wrote to them.
_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

# Fallback rotation settings, used only if a config object is missing the
# corresponding attribute (keeps this module usable with partial configs).
_DEFAULT_LOG_MAX_BYTES = 10 * 1024 * 1024  # 10MB
_DEFAULT_LOG_BACKUP_COUNT = 5


def configure_logging(cfg):
    """
    Attach rotating file handlers to the "app", "hardware" and root/"error"
    loggers, using file paths and rotation settings from the active config
    class (§24, §27).

    Safe to call multiple times - only the first call actually attaches
    handlers, so importing this from several modules (models.py,
    database.py, app.py) never produces duplicate log lines.

    On failure to initialize file-based logging (e.g. unwritable LOG_DIR),
    falls back to console logging rather than raising, so a logging
    misconfiguration can never crash the whole application.
    """
    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return

    formatter = logging.Formatter(_LOG_FORMAT)

    try:
        os.makedirs(cfg.LOG_DIR, exist_ok=True)

        max_bytes = getattr(cfg, "LOG_MAX_BYTES", _DEFAULT_LOG_MAX_BYTES)
        backup_count = getattr(cfg, "LOG_BACKUP_COUNT", _DEFAULT_LOG_BACKUP_COUNT)

        # --- "app" channel: general application / request activity ---
        app_handler = RotatingFileHandler(
            cfg.APP_LOG_FILE, maxBytes=max_bytes, backupCount=backup_count
        )
        app_handler.setFormatter(formatter)
        app_handler.setLevel(logging.INFO)

        app_logger = logging.getLogger("app")
        app_logger.setLevel(logging.INFO)
        app_logger.addHandler(app_handler)

        # Also feed werkzeug's request logs into the same file, matching
        # the behavior already established in app.py::configure_logging.
        logging.getLogger("werkzeug").addHandler(app_handler)

        # --- "hardware" channel: KC868 driver / poller diagnostics ---
        hw_handler = RotatingFileHandler(
            cfg.HARDWARE_LOG_FILE, maxBytes=max_bytes, backupCount=backup_count
        )
        hw_handler.setFormatter(formatter)
        hw_handler.setLevel(logging.DEBUG)

        hw_logger = logging.getLogger("hardware")
        hw_logger.setLevel(logging.DEBUG)
        hw_logger.addHandler(hw_handler)

        # --- "error" channel: root logger, error-level and above ---
        err_handler = RotatingFileHandler(
            cfg.ERROR_LOG_FILE, maxBytes=max_bytes, backupCount=backup_count
        )
        err_handler.setFormatter(formatter)
        err_handler.setLevel(logging.ERROR)

        root_logger = logging.getLogger()
        root_logger.addHandler(err_handler)
        root_logger.setLevel(logging.INFO)

        _LOGGING_CONFIGURED = True

    except OSError as exc:
        # Logging infrastructure failed to initialize (e.g. bad LOG_DIR
        # permissions). Fall back to console-only logging so the app can
        # still surface the problem instead of crashing silently.
        logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT)
        logging.getLogger("app").error(
            "Failed to configure file logging, falling back to console: %s", exc
        )
        _LOGGING_CONFIGURED = True


def get_logger(name):
    """
    Return a named logger for use throughout the codebase.

    If configure_logging() has not yet been called (e.g. this module is
    imported before the Flask app factory runs, such as from a standalone
    script or test), attach a basic console handler as a safe default so
    log calls never raise or get silently dropped.
    """
    if not _LOGGING_CONFIGURED and not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT)

    return logging.getLogger(name)


def is_configured():
    """Expose configuration state, useful for diagnostics/tests."""
    return _LOGGING_CONFIGURED
