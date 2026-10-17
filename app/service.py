import json
import logging
import queue
import threading
import uuid
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import select
from .domain import command_view, device_view, event_timestamp, normalize_telemetry
from .models import Command, Device, Telemetry, now_ms
from .schemas import AckIn, StatusIn, TelemetryIn

log = logging.getLogger(__name__)


class Service:
    def __init__(self, sessions, settings, mqtt, weather, firebase):
        self.sessions, self.settings = sessions, settings
        self.mqtt, self.weather, self.firebase = mqtt, weather, firebase
        self.unmatched = set()
        self.lock = threading.RLock()
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def dispatch(self, device_id, request, uid):
        with self.lock, self.sessions() as db:
            d = db.get(Device, device_id)
            if d is None:
                raise HTTPException(404, 'Device not found')
            value = request.model_dump(exclude_none=True).get('value')
            if request.command == 'UPDATE_SETTINGS':
                # Only a live PENDING command blocks. A TIMEOUT one (device offline, or it dropped
                # an unparsable command and never ACKed) must not lock settings forever.
                for c in db.scalars(select(Command).where(Command.device_id == d.id,
                        Command.status == 'PENDING')):
                    if c.payload['command'] == 'UPDATE_SETTINGS':
                        raise HTTPException(409, 'Previous settings command is unresolved')
                merged = {**d.settings, **value}
                if not merged['warningCm'] < merged['dangerCm'] < merged['criticalCm']:
                    raise HTTPException(422, 'Require warningCm < dangerCm < criticalCm')
            if not self.mqtt.connected:
                raise HTTPException(503, 'MQTT broker is disconnected')
            payload = {'commandId': str(uuid.uuid4()), 'command': request.command}
            if value is not None:
                payload['value'] = value
            c = Command(id=payload['commandId'], device_id=d.id, requester_uid=uid, payload=payload)
            db.add(c)
            db.commit()  # Persist before publishing so a fast ACK can always be correlated.
            try:
                self.mqtt.publish(f'iot/{d.region}/{d.id}/cmd', payload)
            except Exception:
                c.status, c.message, c.completed_ms = 'FAILED', 'MQTT publish failed', now_ms()
                db.commit()
                raise HTTPException(503, {'message': c.message, 'commandId': c.id})
            return command_view(c)

    def ingest(self, topic, raw, retained=False, received=None):
        received = received or now_ms()
        parts = topic.split('/')
        if len(parts) != 4 or parts[0] != 'iot' or parts[3] not in ('telemetry', 'ack', 'status'):
            return
        _, region, device_id, channel = parts
        try:
            data = {'telemetry': TelemetryIn, 'ack': AckIn, 'status': StatusIn}[channel].model_validate(json.loads(raw))
        except (json.JSONDecodeError, TypeError, ValidationError):
            log.warning('Malformed MQTT %s payload discarded for %s', channel, topic)
            return
        if data.deviceId != device_id:
            return
        # Weather I/O happens before the lock so commands can still be sent promptly.
        with self.sessions() as db:
            d = db.get(Device, device_id)
            if d is None or d.region != region:
                # Typically an unregistered device or a region typo; log once per topic, not per message.
                if topic not in self.unmatched:
                    self.unmatched.add(topic)
                    log.warning('MQTT %s ignored: device %s not registered in region %s', channel, device_id, region)
                return
            location = (d.latitude, d.longitude)
        weather = self.weather.rainfall(*location) if channel == 'telemetry' else None
        timestamp, source = event_timestamp(data.timestamp, received)
        with self.lock, self.sessions() as db:
            d = db.get(Device, device_id)
            if channel == 'telemetry':
                value = normalize_telemetry(
                    data.model_dump(), weather, timestamp, source, received, d.settings)
                db.add(Telemetry(device_id=d.id, timestamp_ms=timestamp, data=value))
                if not d.latest or timestamp >= d.latest['timestamp']:
                    d.latest = value
                if not retained:
                    d.last_seen_ms, d.is_online = received, True
            elif channel == 'status':
                if not data.isOnline:
                    d.is_online = False
                elif retained:
                    # A retained online message may be days old; uptime fallback cannot prove freshness.
                    fresh = source == 'device' and 0 <= received - timestamp <= self.settings.offline_after_seconds * 1000
                    d.is_online = fresh
                    if fresh:
                        d.last_seen_ms = timestamp
                else:
                    d.is_online, d.last_seen_ms = True, received
            else:
                if retained:
                    return
                c = db.get(Command, data.commandId)
                if c is None or c.device_id != d.id or c.status not in ('PENDING', 'TIMEOUT'):
                    return
                c.status, c.message, c.completed_ms = data.status, data.message, timestamp
                c.mirror_dirty = True
                if c.status == 'SUCCESS' and c.payload['command'] == 'UPDATE_SETTINGS':
                    # A late ACK of an older command must not regress newer confirmed settings.
                    newer = db.scalars(select(Command).where(
                        Command.device_id == d.id, Command.status == 'SUCCESS',
                        Command.created_ms > c.created_ms, Command.id != c.id)).all()
                    if not any(n.payload['command'] == 'UPDATE_SETTINGS' for n in newer):
                        d.settings = {**d.settings, **c.payload['value']}
            d.mirror_dirty = True
            db.commit()

    def maintain(self):
        current = now_ms()
        with self.lock, self.sessions() as db:
            for c in db.scalars(select(Command).where(Command.status == 'PENDING',
                    Command.created_ms < current - self.settings.command_timeout_seconds * 1000)):
                c.status, c.completed_ms = 'TIMEOUT', current
                c.message = 'No ACK received; execution is unknown'
                c.mirror_dirty = True
            for d in db.scalars(select(Device).where(Device.is_online == True)):
                if d.last_seen_ms is None or current - d.last_seen_ms > self.settings.offline_after_seconds * 1000:
                    d.is_online, d.mirror_dirty = False, True
            db.commit()

    def mirror(self):
        if not self.firebase.enabled:
            return
        # Lock covers each snapshot and dirty reset, preventing lost updates during writes.
        with self.lock, self.sessions() as db:
            for d in db.scalars(select(Device).where(Device.mirror_dirty == True).limit(50)):
                self.firebase.write(f'users/{d.owner_uid}/devices/{d.id}', device_view(d))
                d.mirror_dirty = False
            for c in db.scalars(select(Command).where(Command.mirror_dirty == True).limit(100)):
                d = db.get(Device, c.device_id)
                self.firebase.write(f'users/{d.owner_uid}/commands/{c.id}', command_view(c))
                c.mirror_dirty = False
            db.commit()

    def run(self):
        import time
        last_mirror = 0
        while not self.stop.is_set():
            try:
                message = self.mqtt.messages.get(timeout=0.2)
                try:
                    self.ingest(*message)
                except Exception:
                    log.exception('MQTT processing failed')
            except queue.Empty:
                pass
            try:
                self.maintain()
                if time.monotonic() - last_mirror >= 5:
                    last_mirror = time.monotonic()
                    self.mirror()
            except Exception:
                log.exception('Maintenance/mirror failed; pending mirror writes will retry')
