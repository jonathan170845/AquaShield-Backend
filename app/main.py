import asyncio
import logging
import secrets
from contextlib import asynccontextmanager
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select, text
from sqlalchemy.exc import OperationalError
from .config import Settings
from .domain import command_view, device_view
from .integrations import Firebase, MQTT, Weather
from .models import Command, Device, Telemetry, database
from .schemas import CommandRequest, DeviceCreate, DeviceSettings
from .service import Service

bearer = HTTPBearer(auto_error=False)
log = logging.getLogger(__name__)
DATABASE_STARTUP_ATTEMPTS = 30


def identity(request: Request, credentials: HTTPAuthorizationCredentials | None = Depends(bearer)):
    if credentials is None:
        raise HTTPException(401, 'Bearer token required')
    settings = request.app.state.settings
    if settings.auth_mode == 'development':
        if not secrets.compare_digest(credentials.credentials, settings.dev_api_token):
            raise HTTPException(401, 'Invalid token')
        return {'uid': 'local-admin', 'admin': True}
    try:
        return request.app.state.service.firebase.verify(credentials.credentials)
    except Exception:
        raise HTTPException(401, 'Invalid or revoked Firebase ID token')


def require_admin(user):
    if user.get('admin') is not True:
        raise HTTPException(403, 'Admin role required')


def accessible(db, device_id, user):
    device = db.get(Device, device_id)
    if device is None or (device.owner_uid != user['uid'] and user.get('admin') is not True):
        raise HTTPException(404, 'Device not found')
    return device


async def connect_database(settings):
    """Wait through the one-time MySQL container initialization window."""
    for attempt in range(1, DATABASE_STARTUP_ATTEMPTS + 1):
        try:
            return database(settings.database_url)
        except OperationalError:
            if attempt == DATABASE_STARTUP_ATTEMPTS:
                raise
            log.warning('Database is not ready; retrying connection (%s/%s)',
                        attempt, DATABASE_STARTUP_ATTEMPTS)
            await asyncio.sleep(1)


def create_app(settings=None, service=None):
    @asynccontextmanager
    async def lifespan(app):
        cfg = settings or Settings()
        engine = None
        svc = service
        if svc is None:
            firebase = Firebase(cfg)
            engine, sessions = await connect_database(cfg)
            svc = Service(sessions, cfg, MQTT(cfg), Weather(cfg.weather_cache_seconds), firebase)
        app.state.settings, app.state.service = cfg, svc
        svc.mqtt.start()
        svc.thread.start()
        try:
            yield
        finally:
            svc.mqtt.close()
            svc.stop.set()
            svc.thread.join()
            svc.weather.client.close()
            svc.firebase.close()
            if engine:
                engine.dispose()

    app = FastAPI(title='AquaShield API', version='1.0.0', lifespan=lifespan)
    cfg = settings or Settings()
    app.add_middleware(CORSMiddleware, allow_origins=cfg.cors_origins,
                       allow_methods=['GET', 'POST', 'PUT'], allow_headers=['Authorization', 'Content-Type'])

    @app.get('/health')
    def health(request: Request):
        svc = request.app.state.service
        with svc.sessions() as db:
            db.execute(text('SELECT 1'))
        return {'status': 'ok', 'mqttConnected': svc.mqtt.connected,
                'firebaseMirrorEnabled': svc.firebase.enabled}

    @app.post('/devices', status_code=201)
    def register(body: DeviceCreate, request: Request, user=Depends(identity)):
        require_admin(user)
        svc = request.app.state.service
        with svc.lock, svc.sessions() as db:
            if db.get(Device, body.deviceId):
                raise HTTPException(409, 'Device already exists')
            device = Device(id=body.deviceId, region=body.region, name=body.name,
                            owner_uid=body.ownerUid, latitude=body.latitude, longitude=body.longitude)
            db.add(device)
            db.commit()
            return device_view(device)

    @app.get('/devices')
    def devices(request: Request, limit: int = Query(100, ge=1, le=500),
                offset: int = Query(0, ge=0), user=Depends(identity)):
        with request.app.state.service.sessions() as db:
            query = select(Device)
            if user.get('admin') is not True:
                query = query.where(Device.owner_uid == user['uid'])
            return [device_view(d) for d in db.scalars(query.order_by(Device.id).offset(offset).limit(limit))]

    @app.get('/devices/{device_id}')
    def detail(device_id: str, request: Request, user=Depends(identity)):
        with request.app.state.service.sessions() as db:
            return device_view(accessible(db, device_id, user))

    @app.get('/devices/{device_id}/telemetry')
    def history(device_id: str, request: Request, since: int | None = Query(None, ge=0),
                until: int | None = Query(None, ge=0), limit: int = Query(100, ge=1, le=1000),
                offset: int = Query(0, ge=0), user=Depends(identity)):
        if since is not None and until is not None and since > until:
            raise HTTPException(422, 'since must be <= until')
        with request.app.state.service.sessions() as db:
            accessible(db, device_id, user)
            query = select(Telemetry).where(Telemetry.device_id == device_id)
            if since is not None:
                query = query.where(Telemetry.timestamp_ms >= since)
            if until is not None:
                query = query.where(Telemetry.timestamp_ms <= until)
            return [r.data for r in db.scalars(query.order_by(Telemetry.timestamp_ms.desc(), Telemetry.id.desc()).offset(offset).limit(limit))]

    def send(device_id, body, request, user):
        svc = request.app.state.service
        with svc.sessions() as db:
            accessible(db, device_id, user)
        if body.command in ('EMERGENCY', 'UPDATE_SETTINGS'):
            require_admin(user)
        return svc.dispatch(device_id, body, user['uid'])

    @app.post('/devices/{device_id}/commands', status_code=202)
    def command(device_id: str, body: CommandRequest, request: Request, user=Depends(identity)):
        return send(device_id, body, request, user)

    @app.put('/devices/{device_id}/settings', status_code=202)
    def settings_update(device_id: str, body: DeviceSettings, request: Request, user=Depends(identity)):
        return send(device_id, CommandRequest(command='UPDATE_SETTINGS', value=body), request, user)

    @app.get('/devices/{device_id}/commands')
    def commands(device_id: str, request: Request, limit: int = Query(100, ge=1, le=500),
                 offset: int = Query(0, ge=0), user=Depends(identity)):
        with request.app.state.service.sessions() as db:
            accessible(db, device_id, user)
            return [command_view(c) for c in db.scalars(select(Command).where(Command.device_id == device_id)
                    .order_by(Command.created_ms.desc(), Command.id).offset(offset).limit(limit))]

    @app.get('/commands/{command_id}')
    def command_detail(command_id: str, request: Request, user=Depends(identity)):
        with request.app.state.service.sessions() as db:
            c = db.get(Command, command_id)
            if c is None:
                raise HTTPException(404, 'Command not found')
            accessible(db, c.device_id, user)
            return command_view(c)

    return app


app = create_app()
