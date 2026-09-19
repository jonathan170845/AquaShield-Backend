import json
import logging
import math
import queue
import time
import uuid
import httpx
import paho.mqtt.client as mqtt

log = logging.getLogger(__name__)


class Weather:
    def __init__(self, ttl):
        self.ttl = ttl
        self.cache = {}
        self.client = httpx.Client(timeout=5)

    def rainfall(self, latitude, longitude):
        key = (latitude, longitude)
        cached = self.cache.get(key)
        if cached and time.monotonic() - cached[0] < self.ttl:
            return cached[1]
        result = {'rainfallMmHr': None, 'rainfallSource': 'unavailable', 'rainfallObservedAt': None}
        try:
            response = self.client.get('https://api.open-meteo.com/v1/forecast', params={
                'latitude': latitude, 'longitude': longitude, 'hourly': 'rain,showers',
                'past_hours': 2, 'forecast_hours': 1, 'timeformat': 'unixtime', 'timezone': 'UTC'})
            response.raise_for_status()
            hourly = response.json()['hourly']
            candidates = [i for i, t in enumerate(hourly['time']) if t <= time.time()]
            i = candidates[-1]
            observed = hourly['time'][i]
            rain = float(hourly['rain'][i]) + float(hourly['showers'][i])
            if not math.isfinite(rain) or rain < 0 or time.time() - observed > 7200:
                raise ValueError('Invalid weather data')
            result = {'rainfallMmHr': rain, 'rainfallSource': 'open-meteo-hourly-model',
                      'rainfallObservedAt': observed * 1000}
        except (httpx.HTTPError, ValueError, KeyError, IndexError, TypeError):
            log.warning('Weather unavailable; storing rainfall as N/A')
        self.cache[key] = (time.monotonic(), result)
        return result


class Firebase:
    def __init__(self, settings):
        self.enabled = settings.firebase_enabled
        self.app = None
        if self.enabled or settings.auth_mode == 'firebase':
            import firebase_admin
            from firebase_admin import credentials
            self.app = firebase_admin.initialize_app(
                credentials.Certificate(settings.firebase_credentials_path),
                {'databaseURL': settings.firebase_database_url, 'httpTimeout': 5} if self.enabled else {},
                name='aquashield-' + uuid.uuid4().hex)

    def verify(self, token):
        from firebase_admin import auth
        return auth.verify_id_token(token, app=self.app, check_revoked=True)

    def write(self, path, value):
        if self.enabled:
            from firebase_admin import db
            db.reference(path, app=self.app).set(value)

    def close(self):
        if self.app:
            import firebase_admin
            firebase_admin.delete_app(self.app)


class MQTT:
    def __init__(self, settings):
        self.settings = settings
        self.messages = queue.Queue(maxsize=10000)
        self.connected = False
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                                  client_id='aquashield-backend-' + uuid.uuid4().hex[:12],
                                  transport=settings.mqtt_transport)
        if settings.mqtt_username:
            self.client.username_pw_set(settings.mqtt_username, settings.mqtt_password)
        if settings.mqtt_tls:
            self.client.tls_set()
        if settings.mqtt_transport == 'websockets':
            self.client.ws_set_options(path=settings.mqtt_websocket_path)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.client.reconnect_delay_set(1, 30)

    def on_connect(self, client, userdata, flags, reason_code, properties):
        self.connected = not reason_code.is_failure
        if self.connected:
            client.subscribe([(f'iot/+/+/{channel}', 1) for channel in ('telemetry', 'ack', 'status')])

    def on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.connected = False

    def on_message(self, client, userdata, message):
        if len(message.payload) > 16384:
            log.warning('Oversized MQTT payload discarded')
            return
        try:
            self.messages.put_nowait((message.topic, message.payload, message.retain,
                                      time.time_ns() // 1_000_000))
        except queue.Full:
            log.error('MQTT input queue full; message discarded')

    def start(self):
        if self.settings.mqtt_enabled:
            self.client.connect_async(self.settings.mqtt_host, self.settings.mqtt_port, 60)
            self.client.loop_start()

    def publish(self, topic, payload):
        if not self.connected:
            raise RuntimeError('MQTT is disconnected')
        result = self.client.publish(topic, json.dumps(payload), qos=0, retain=False)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError('MQTT publish failed')

    def close(self):
        if self.settings.mqtt_enabled:
            self.client.disconnect()
            self.client.loop_stop()
