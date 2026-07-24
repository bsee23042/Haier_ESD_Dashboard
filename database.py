"""
database.py
SQLAlchemy models and database bootstrap for the Haier ESD Monitoring & Dashboard System.

    stations (1) --< esd_events
    users    (1) --< esd_events   (via acknowledged_by)
    users    (1) --< audit_log
    system_health   (independent heartbeat/comm-status log)
    system_config   (key/value runtime settings)

WAL journal mode is enabled so the poller can write events while the
dashboard reads concurrently.
"""

import os
import datetime

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Text,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    event,
    inspect,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, scoped_session
from sqlalchemy.engine import Engine

Base = declarative_base()

# Populated by init_engine(); a module-level scoped session keeps call sites simple
# across the poller thread and Flask request threads.
engine = None
SessionLocal = None


# ------------------------------------------------------------------ #
# Models
# ------------------------------------------------------------------ #
class Station(Base):
    __tablename__ = "stations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    station_code = Column(String(30), unique=True, nullable=False)     # e.g. "ESD-STN-01"
    kc868_channel = Column(Integer, unique=True, nullable=False)       # physical DI channel
    description = Column(String(255), nullable=True)
    status = Column(String(20), nullable=False, default="ACTIVE")      # ACTIVE/DISABLED/MAINTENANCE

    # IMPORTANT: this must be a real mapped column, not just a constructor
    # kwarg. It reflects whether the station is currently in an open
    # VIOLATION (True) or SAFE/RESTORED (False), and is kept up to date by
    # EventService.record_transition() every time a transition is logged.
    current_state = Column(Boolean, nullable=False, default=False)

    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    events = relationship("ESDEvent", back_populates="station", lazy="dynamic")

    def to_dict(self):
        return {
            "id": self.id,
            "station_code": self.station_code,
            "kc868_channel": self.kc868_channel,
            "description": self.description,
            "status": self.status,
            "current_state": self.current_state,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class ESDEvent(Base):
    __tablename__ = "esd_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(Integer, ForeignKey("stations.id", ondelete="CASCADE"), nullable=False)
    event_type = Column(String(20), nullable=False)  # VIOLATION / RESTORED / COMM_LOST
    event_timestamp = Column(DateTime, nullable=False, default=datetime.datetime.utcnow, index=True)
    duration_seconds = Column(Integer, nullable=True)  # set when RESTORED closes a VIOLATION
    acknowledged = Column(Boolean, default=False, nullable=False)
    acknowledged_by = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    acknowledged_at = Column(DateTime, nullable=True)

    station = relationship("Station", back_populates="events")
    acknowledger = relationship("User", foreign_keys=[acknowledged_by])

    __table_args__ = (
        Index("ix_esd_events_station_timestamp", "station_id", "event_timestamp"),
        Index("ix_esd_events_event_type", "event_type"),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "station_id": self.station_id,
            "event_type": self.event_type,
            "event_timestamp": self.event_timestamp.isoformat() if self.event_timestamp else None,
            "duration_seconds": self.duration_seconds,
            "acknowledged": self.acknowledged,
            "acknowledged_by": self.acknowledged_by,
            "acknowledged_at": self.acknowledged_at.isoformat() if self.acknowledged_at else None,
        }


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(50), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    role = Column(String(20), nullable=False, default="VIEWER")  # ADMIN/SUPERVISOR/OPERATOR/VIEWER
    full_name = Column(String(100))
    last_login = Column(DateTime, nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)

    def to_dict(self):
        return {
            "id": self.id,
            "username": self.username,
            "role": self.role,
            "full_name": self.full_name,
            "last_login": self.last_login.isoformat() if self.last_login else None,
            "is_active": self.is_active,
        }


class SystemHealth(Base):
    __tablename__ = "system_health"

    id = Column(Integer, primary_key=True, autoincrement=True)
    check_timestamp = Column(DateTime, nullable=False, default=datetime.datetime.utcnow, index=True)
    kc868_status = Column(String(10), nullable=False)  # ONLINE / OFFLINE
    response_time_ms = Column(Integer, nullable=True)
    notes = Column(Text, nullable=True)

    def to_dict(self):
        return {
            "id": self.id,
            "check_timestamp": self.check_timestamp.isoformat() if self.check_timestamp else None,
            "kc868_status": self.kc868_status,
            "response_time_ms": self.response_time_ms,
            "notes": self.notes,
        }


class AuditLog(Base):
    __tablename__ = "audit_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    action = Column(String(100), nullable=False)  # e.g. "EXPORTED_REPORT", "DISABLED_STATION"
    timestamp = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    details = Column(Text, nullable=True)  # JSON string of context

    user = relationship("User", foreign_keys=[user_id])

    def to_dict(self):
        return {
            "id": self.id,
            "user_id": self.user_id,
            "action": self.action,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "details": self.details,
        }


class SystemConfig(Base):
    __tablename__ = "system_config"

    key = Column(String(50), primary_key=True)   # e.g. "retention_days", "poll_interval_ms"
    value = Column(String(255), nullable=False)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)


# ------------------------------------------------------------------ #
# Engine / session bootstrap
# ------------------------------------------------------------------ #
@event.listens_for(Engine, "connect")
def _enable_sqlite_pragmas(dbapi_connection, connection_record):
    """Enable WAL mode + foreign keys on every new SQLite connection."""
    if type(dbapi_connection).__module__.startswith("sqlite3"):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.close()


def init_engine(database_uri, connect_args=None):
    """Create the global engine + scoped session factory. Call once at startup."""
    global engine, SessionLocal
    if connect_args is None:
        if database_uri and database_uri.startswith("sqlite"):
            connect_args = {"check_same_thread": False}
        else:
            connect_args = {}
    engine = create_engine(database_uri, connect_args=connect_args, future=True)
    SessionLocal = scoped_session(sessionmaker(bind=engine, autoflush=False, autocommit=False))
    return engine, SessionLocal


def _resolve_database_uri():
    """
    Figure out where the database lives (SQLite or PostgreSQL).
    """
    try:
        from config import get_config
        cfg = get_config()
        uri = getattr(cfg, "SQLALCHEMY_DATABASE_URI", None)
        connect_args = None
        engine_options = getattr(cfg, "SQLALCHEMY_ENGINE_OPTIONS", None)
        if engine_options and "connect_args" in engine_options:
            connect_args = engine_options.get("connect_args")
        elif uri and not uri.startswith("sqlite"):
            connect_args = {}
            
        if uri:
            db_dir = getattr(cfg, "DATABASE_DIR", None)
            if db_dir and uri.startswith("sqlite"):
                os.makedirs(db_dir, exist_ok=True)
            return uri, connect_args
    except Exception:
        pass

    base_dir = os.path.dirname(os.path.abspath(__file__))
    db_dir = os.path.join(base_dir, "database")
    os.makedirs(db_dir, exist_ok=True)
    return f"sqlite:///{os.path.join(db_dir, 'esd_monitoring.db')}", {"check_same_thread": False}


def run_migrations(eng):
    if eng.dialect.name != "sqlite":
        return
    inspector = inspect(eng)
    if not inspector.has_table("esd_events"):
        return

    # Check foreign keys of esd_events
    with eng.begin() as conn:
        from sqlalchemy import text
        fks = conn.execute(text("PRAGMA foreign_key_list(esd_events)")).fetchall()
        needs_mig = False
        for fk in fks:
            # fk[2] is target table name (e.g. 'stations'), fk[6] is on_delete action
            if fk[2] == "stations" and fk[6] != "CASCADE":
                needs_mig = True
                break
            if fk[2] == "users" and fk[6] != "SET NULL":
                needs_mig = True
                break

        if needs_mig:
            # We must recreate the esd_events table to support ON DELETE CASCADE / SET NULL
            # Since SQLite doesn't support changing constraints, we rename, create new, copy, and drop.
            conn.execute(text("PRAGMA foreign_keys=OFF"))
            try:
                # Rename old table
                conn.execute(text("ALTER TABLE esd_events RENAME TO _esd_events_old"))
                
                # Drop existing indices to avoid name clash
                conn.execute(text("DROP INDEX IF EXISTS ix_esd_events_station_timestamp"))
                conn.execute(text("DROP INDEX IF EXISTS ix_esd_events_event_type"))
                conn.execute(text("DROP INDEX IF EXISTS ix_esd_events_event_timestamp"))
                
                # Re-create table using SQLAlchemy schema
                Base.metadata.tables["esd_events"].create(conn)
                
                # Copy data (matching columns)
                conn.execute(text("""
                    INSERT INTO esd_events (id, station_id, event_type, event_timestamp, duration_seconds, acknowledged, acknowledged_by, acknowledged_at)
                    SELECT id, station_id, event_type, event_timestamp, duration_seconds, acknowledged, acknowledged_by, acknowledged_at
                    FROM _esd_events_old
                """))
                
                # Drop old table
                conn.execute(text("DROP TABLE _esd_events_old"))
            finally:
                conn.execute(text("PRAGMA foreign_keys=ON"))

    if inspector.has_table("audit_log"):
        with eng.begin() as conn:
            from sqlalchemy import text
            fks = conn.execute(text("PRAGMA foreign_key_list(audit_log)")).fetchall()
            needs_mig = False
            for fk in fks:
                if fk[2] == "users" and fk[6] != "SET NULL":
                    needs_mig = True
                    break
            
            if needs_mig:
                conn.execute(text("PRAGMA foreign_keys=OFF"))
                try:
                    conn.execute(text("ALTER TABLE audit_log RENAME TO _audit_log_old"))
                    Base.metadata.tables["audit_log"].create(conn)
                    conn.execute(text("""
                        INSERT INTO audit_log (id, user_id, action, timestamp, details)
                        SELECT id, user_id, action, timestamp, details
                        FROM _audit_log_old
                    """))
                    conn.execute(text("DROP TABLE _audit_log_old"))
                finally:
                    conn.execute(text("PRAGMA foreign_keys=ON"))


def init_db():
    """
    One-time schema creation + bootstrap. Safe to call with no arguments -
    it will use config.get_config() if available, otherwise defaults to a
    local ./database/esd_monitoring.db SQLite file.
    """
    database_uri, connect_args = _resolve_database_uri()
    eng, _ = init_engine(database_uri, connect_args=connect_args)
    
    # Clean up schema mismatch if columns are missing from existing SQLite files on disk
    inspector = inspect(eng)
    if inspector.has_table("stations"):
        columns = [col["name"] for col in inspector.get_columns("stations")]
        if "description" not in columns or "current_state" not in columns:
            Base.metadata.drop_all(eng)
            
    # Run migration if needed
    run_migrations(eng)
            
    Base.metadata.create_all(eng)
    return eng


def get_session():
    """Return the current scoped session. Call init_db() first."""
    if SessionLocal is None:
        raise RuntimeError("Database not initialized - call init_db() first.")
    return SessionLocal()


def remove_session():
    """Release the scoped session at the end of a request/thread task."""
    if SessionLocal is not None:
        SessionLocal.remove()