import time
from sqlalchemy import JSON, BigInteger, Boolean, Float, ForeignKey, Integer, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

DEFAULT_DEVICE_SETTINGS = {
    'warningCm': 10.0,
    'dangerCm': 25.0,
    'criticalCm': 40.0,
    'telemetryIntervalSec': 5,
}


def now_ms():
    return time.time_ns() // 1_000_000


class Base(DeclarativeBase):
    pass


class Device(Base):
    __tablename__ = 'devices'
    id: Mapped[str] = mapped_column(String(80), primary_key=True)
    region: Mapped[str] = mapped_column(String(80))
    name: Mapped[str] = mapped_column(String(120))
    owner_uid: Mapped[str] = mapped_column(String(128), index=True)
    latitude: Mapped[float] = mapped_column(Float)
    longitude: Mapped[float] = mapped_column(Float)
    settings: Mapped[dict] = mapped_column(JSON, default=lambda: DEFAULT_DEVICE_SETTINGS.copy())
    is_online: Mapped[bool] = mapped_column(Boolean, default=False)
    last_seen_ms: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    latest: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    mirror_dirty: Mapped[bool] = mapped_column(Boolean, default=True)


class Telemetry(Base):
    __tablename__ = 'telemetry'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[str] = mapped_column(ForeignKey('devices.id'), index=True)
    timestamp_ms: Mapped[int] = mapped_column(BigInteger, index=True)
    data: Mapped[dict] = mapped_column(JSON)


class Command(Base):
    __tablename__ = 'commands'
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    device_id: Mapped[str] = mapped_column(ForeignKey('devices.id'), index=True)
    requester_uid: Mapped[str] = mapped_column(String(128))
    payload: Mapped[dict] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(20), default='PENDING', index=True)
    created_ms: Mapped[int] = mapped_column(BigInteger, default=now_ms)
    completed_ms: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    message: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    mirror_dirty: Mapped[bool] = mapped_column(Boolean, default=True)


def database(url):
    options = {'check_same_thread': False} if url.startswith('sqlite') else {}
    engine = create_engine(url, connect_args=options, pool_pre_ping=True)
    try:
        Base.metadata.create_all(engine)
    except Exception:
        engine.dispose()
        raise
    return engine, sessionmaker(engine, expire_on_commit=False)
