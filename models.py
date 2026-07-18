"""
models.py
Data-access (repository) layer for the Haier ESD Monitoring & Dashboard System.

This module sits between the Service/Controller layers (app.py) and the raw
SQLAlchemy schema in database.py (§10-12). It intentionally does NOT redefine
any ORM model classes or table metadata - database.py remains the single
source of truth for the schema (Station, ESDEvent, User, SystemHealth,
AuditLog, SystemConfig) and its engine/session bootstrap (init_db,
init_engine, get_session, remove_session). Re-declaring those classes here
would register duplicate SQLAlchemy mappers against the same table names and
break at import time, so instead this module re-exports them for convenient
access and wraps the most common query patterns as small repository classes.

Rationale (kept consistent with the layering already used in app.py):
    Poller/Driver Layer   -> app.py (KC868Driver / SimulatedKC868Driver)
    Service Layer         -> app.py (EventService, DebounceService)
    Controller/Route L.   -> app.py (Flask routes)
    Data Access / Model   -> database.py (schema) + models.py (repositories)

Every repository method opens no new session lifecycle of its own by
default - callers pass in an active session (typically obtained via
database.get_session()) so transaction boundaries stay controlled by the
caller, exactly as app.py's routes/services already do. This avoids
surprising commits/rollbacks hidden inside a repository call.
"""

import datetime

from sqlalchemy import func

import database as db
from logger import get_logger

log = get_logger("models")

# ------------------------------------------------------------------ #
# Re-exports - lets other modules do `from models import Station` etc.
# without needing to know the schema lives in database.py.
# ------------------------------------------------------------------ #
Station = db.Station
ESDEvent = db.ESDEvent
User = db.User
SystemHealth = db.SystemHealth
AuditLog = db.AuditLog
SystemConfig = db.SystemConfig


# ==================================================================== #
# StationRepository
# ==================================================================== #
class StationRepository:
    """Query helpers for the `stations` table (§10-11)."""

    @staticmethod
    def get_by_id(session_ref, station_id):
        """Return a Station by primary key, or None if it does not exist."""
        try:
            return session_ref.get(db.Station, station_id)
        except Exception as exc:  # noqa: BLE001 - never let a lookup crash the caller
            log.error("StationRepository.get_by_id(%s) failed: %s", station_id, exc)
            raise

    @staticmethod
    def get_by_code(session_ref, station_code):
        """Return a Station by its human-readable code (e.g. 'ST-01')."""
        return session_ref.query(db.Station).filter_by(station_code=station_code).first()

    @staticmethod
    def get_by_channel(session_ref, kc868_channel):
        """Return the Station mapped to a given KC868 discrete-input channel."""
        return session_ref.query(db.Station).filter_by(kc868_channel=kc868_channel).first()

    @staticmethod
    def list_active(session_ref):
        """Return all ACTIVE stations ordered by their physical channel number."""
        return (
            session_ref.query(db.Station)
            .filter_by(status="ACTIVE")
            .order_by(db.Station.kc868_channel)
            .all()
        )

    @staticmethod
    def list_all(session_ref):
        """Return every station regardless of status, ordered by channel."""
        return session_ref.query(db.Station).order_by(db.Station.kc868_channel).all()

    @staticmethod
    def active_channel_map(session_ref):
        """
        Return {kc868_channel: Station} for ACTIVE stations only.
        Used by the poller (PollerThread.poll_once) to resolve raw channel
        reads to the station they belong to without a per-channel query.
        """
        stations = StationRepository.list_active(session_ref)
        return {s.kc868_channel: s for s in stations}


# ==================================================================== #
# ESDEventRepository
# ==================================================================== #
class ESDEventRepository:
    """Query helpers for the `esd_events` table (§2, §5, §23)."""

    @staticmethod
    def get_by_id(session_ref, event_id):
        return session_ref.get(db.ESDEvent, event_id)

    @staticmethod
    def latest_for_station(session_ref, station_id):
        """Most recent event (any type) for a given station, or None."""
        return (
            session_ref.query(db.ESDEvent)
            .filter_by(station_id=station_id)
            .order_by(db.ESDEvent.event_timestamp.desc())
            .first()
        )

    @staticmethod
    def latest_open_violation(session_ref, station_id):
        """
        Most recent unresolved VIOLATION event for a station (i.e. the one
        that a subsequent RESTORED transition would close out). Mirrors the
        lookup performed inline in app.py::EventService.record_transition.
        """
        return (
            session_ref.query(db.ESDEvent)
            .filter_by(station_id=station_id, event_type="VIOLATION")
            .order_by(db.ESDEvent.event_timestamp.desc())
            .first()
        )

    @staticmethod
    def query_filtered(session_ref, station_id=None, event_type=None,
                        date_from=None, date_to=None, limit=1000):
        """
        Build a filtered esd_events query matching the same parameters
        accepted by app.py's GET /api/events route, so the route handler
        (or any future consumer, e.g. scheduled reports) can share one
        implementation instead of duplicating filter logic.
        """
        query = session_ref.query(db.ESDEvent)

        if station_id:
            query = query.filter_by(station_id=station_id)
        if event_type:
            query = query.filter_by(event_type=event_type)
        if date_from:
            query = query.filter(db.ESDEvent.event_timestamp >= date_from)
        if date_to:
            query = query.filter(db.ESDEvent.event_timestamp <= date_to)

        return query.order_by(db.ESDEvent.event_timestamp.desc()).limit(limit).all()

    @staticmethod
    def unacknowledged(session_ref, limit=1000):
        """Return VIOLATION events that have not yet been acknowledged."""
        return (
            session_ref.query(db.ESDEvent)
            .filter_by(event_type="VIOLATION", acknowledged=False)
            .order_by(db.ESDEvent.event_timestamp.desc())
            .limit(limit)
            .all()
        )

    @staticmethod
    def purge_older_than(session_ref, cutoff_timestamp):
        """
        Delete esd_events rows older than cutoff_timestamp (retention policy,
        §5/§28). Returns the number of rows deleted. Caller is responsible
        for archiving beforehand if RETENTION_ARCHIVE_BEFORE_DELETE is set,
        and for committing the session afterward.
        """
        try:
            deleted = (
                session_ref.query(db.ESDEvent)
                .filter(db.ESDEvent.event_timestamp < cutoff_timestamp)
                .delete(synchronize_session=False)
            )
            log.info("Purged %s esd_events rows older than %s", deleted, cutoff_timestamp)
            return deleted
        except Exception as exc:  # noqa: BLE001
            log.error("ESDEventRepository.purge_older_than failed: %s", exc)
            raise


# ==================================================================== #
# UserRepository
# ==================================================================== #
class UserRepository:
    """Query helpers for the `users` table (§29)."""

    @staticmethod
    def get_by_id(session_ref, user_id):
        return session_ref.get(db.User, user_id)

    @staticmethod
    def get_active_by_username(session_ref, username):
        """Return an active User by username, used by the /login route."""
        return session_ref.query(db.User).filter_by(username=username, is_active=True).first()

    @staticmethod
    def has_role(session_ref, user_id, *roles):
        """Return True if the user with `user_id` currently holds one of `roles`."""
        user = UserRepository.get_by_id(session_ref, user_id)
        return bool(user and user.role in roles)

    @staticmethod
    def any_admin_exists(session_ref):
        """Used by app.py's _bootstrap_admin_if_missing() first-run check."""
        return session_ref.query(db.User).filter_by(role="ADMIN").first() is not None


# ==================================================================== #
# SystemHealthRepository
# ==================================================================== #
class SystemHealthRepository:
    """Query helpers for the `system_health` heartbeat/comm-status log."""

    @staticmethod
    def latest(session_ref):
        return (
            session_ref.query(db.SystemHealth)
            .order_by(db.SystemHealth.check_timestamp.desc())
            .first()
        )

    @staticmethod
    def is_currently_offline(session_ref):
        latest = SystemHealthRepository.latest(session_ref)
        return bool(latest and latest.kc868_status == "OFFLINE")


# ==================================================================== #
# AuditLogRepository
# ==================================================================== #
class AuditLogRepository:
    """Query helpers for the `audit_log` table (§29)."""

    @staticmethod
    def record(session_ref, user_id, action, details=None):
        """
        Convenience helper to append an audit entry. Does not commit -
        callers batch this into the same transaction as the action being
        audited (matching the pattern already used throughout app.py).
        """
        entry = db.AuditLog(user_id=user_id, action=action, details=details)
        session_ref.add(entry)
        return entry

    @staticmethod
    def recent(session_ref, limit=200):
        return (
            session_ref.query(db.AuditLog)
            .order_by(db.AuditLog.timestamp.desc())
            .limit(limit)
            .all()
        )


# ==================================================================== #
# SystemConfigRepository
# ==================================================================== #
class SystemConfigRepository:
    """Key/value accessors for the `system_config` runtime settings table."""

    @staticmethod
    def get(session_ref, key, default=None):
        row = session_ref.get(db.SystemConfig, key)
        return row.value if row is not None else default

    @staticmethod
    def set(session_ref, key, value):
        """
        Upsert a system_config row. Caller commits. Timestamps are handled
        automatically by the column's onupdate/default in database.py.
        """
        row = session_ref.get(db.SystemConfig, key)
        if row is None:
            row = db.SystemConfig(key=key, value=str(value))
            session_ref.add(row)
        else:
            row.value = str(value)
        return row

    @staticmethod
    def get_int(session_ref, key, default=0):
        """Typed convenience accessor - system_config values are stored as strings."""
        raw = SystemConfigRepository.get(session_ref, key)
        if raw is None:
            return default
        try:
            return int(raw)
        except (TypeError, ValueError) as exc:
            log.warning("SystemConfig[%s]=%r is not an int, using default %s (%s)",
                        key, raw, default, exc)
            return default


# ==================================================================== #
# Aggregate / reporting helpers
# ==================================================================== #
def violation_counts_by_station(session_ref, date_from=None, date_to=None):
    """
    Return a list of (station_id, violation_count) tuples for VIOLATION
    events in the given window. Backing helper for future reporting/export
    features (§29) without adding another ad-hoc query inline in app.py.
    """
    query = (
        session_ref.query(db.ESDEvent.station_id, func.count(db.ESDEvent.id))
        .filter(db.ESDEvent.event_type == "VIOLATION")
    )
    if date_from:
        query = query.filter(db.ESDEvent.event_timestamp >= date_from)
    if date_to:
        query = query.filter(db.ESDEvent.event_timestamp <= date_to)

    return query.group_by(db.ESDEvent.station_id).all()


def current_station_status(session_ref, station):
    """
    Derive a station's current status string ("OK" / "VIOLATION") from its
    most recent event, matching the logic inline in app.py::api_live_status
    and api_stations_status, so both routes and any future consumer share
    one implementation.
    """
    last_event = ESDEventRepository.latest_for_station(session_ref, station.id)
    if last_event and last_event.event_type == "VIOLATION":
        return "VIOLATION"
    return "OK"
