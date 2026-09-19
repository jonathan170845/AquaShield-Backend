# AquaShield Backend

FastAPI backend for the ESP32-C3 firmware revision of 05 September 2026. The data flow is MQTT → validation and Open-Meteo enrichment → MySQL → Firebase Realtime Database mirror. React Native uses REST for history and control and Firebase listeners for monitoring. Door and pump automation stays in the firmware.

## Run locally

Python 3.13 or newer is required. SQLite supports local development without MySQL.

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements-dev.txt
Copy-Item .env.example .env
```

For a local run without Firebase or MQTT, set this in `.env`:

```dotenv
AUTH_MODE=development
DEV_API_TOKEN=set-a-random-local-token-with-at-least-24-characters
FIREBASE_ENABLED=false
MQTT_ENABLED=false
```

```powershell
.\.venv\Scripts\python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

Open http://127.0.0.1:8000/docs and select **Authorize** to enter the local token. Development mode grants admin access. Commands return HTTP 503 while MQTT is disabled. `/health` reports SQL and MQTT connectivity.

## Firebase and MQTT configuration

A `type: service_account` JSON file is a server credential, not a database schema. Store it at `secrets/service-account.json`; never include it in a mobile app or source control. `.env`, `secrets/`, service-account JSON files, and local databases are ignored by Git and Docker builds.

Enable Firebase Authentication for mobile login and get the **Realtime Database URL** from Firebase Console. The URL is separate from the JSON credential. Configure `.env` with values supplied by the team and broker administrator:

```dotenv
AUTH_MODE=firebase
FIREBASE_ENABLED=true
FIREBASE_CREDENTIALS_PATH=secrets/service-account.json
FIREBASE_DATABASE_URL=https://YOUR-DATABASE.asia-southeast1.firebasedatabase.app

MQTT_ENABLED=true
MQTT_HOST=mqtt.bifriends.my.id
MQTT_PORT=9001
MQTT_TRANSPORT=websockets
MQTT_WEBSOCKET_PATH=/mqtt
MQTT_TLS=true
MQTT_USERNAME=VALUE_FROM_BROKER_ADMIN
MQTT_PASSWORD=VALUE_FROM_BROKER_ADMIN
```

The above is the current backend connection profile: MQTT over secure WebSocket. The firmware can use a different listener on the same broker. Do not commit MQTT credentials. Mobile clients use the Firebase client SDK, call `user.getIdToken()`, and send `Authorization: Bearer <ID_TOKEN>` to this API. The backend verifies token validity and revocation.

Administrators require the custom claim `admin: true`. Apply it only from a trusted admin environment:

```python
import firebase_admin
from firebase_admin import auth, credentials

firebase_admin.initialize_app(credentials.Certificate('secrets/service-account.json'))
user = auth.get_user('ADMIN_UID')
auth.set_custom_user_claims(user.uid, {**(user.custom_claims or {}), 'admin': True})
```

Refresh the ID token after a claim change. Apply `firebase.database.rules.json` in Firebase Console after adapting it to the team project. The rules allow an owner or admin to read, prevent mobile clients from writing directly, and let the backend use its Admin SDK service account.

Firebase mirror paths:

```text
users/{ownerUid}/devices/{deviceId}
  deviceId, region, name, latitude, longitude
  settings, isOnline, lastSeenAt, latest
users/{ownerUid}/commands/{commandId}
  commandId, deviceId, command, value, status, createdAt, completedAt, message
```

Attach React Native listeners to those paths. Full history stays in SQL. Firebase omits `null` fields, so a missing field also means N/A. Mirror writes are attempted every five seconds and pending writes are retried after failure or restart.

## Device registration and API

An admin registers each device before telemetry is accepted:

```json
POST /devices
{
  "deviceId": "gate-001",
  "region": "jakarta-barat",
  "name": "Home Gate",
  "ownerUid": "FIREBASE_OWNER_UID",
  "latitude": -6.2,
  "longitude": 106.8
}
```

Replace the coordinates with the installation location. In development mode, `ownerUid` can be `local-admin`. The MQTT topic and payload ID must match the registered `deviceId` and `region`.

| Endpoint | Purpose / access |
| --- | --- |
| `GET /health` | SQL and connectivity status; public |
| `POST /devices` | Register a device; admin |
| `GET /devices` | Devices owned by the caller; admins see all |
| `GET /devices/{deviceId}` | Status, active settings, and latest telemetry |
| `GET /devices/{deviceId}/telemetry` | History; filter with `since`/`until` UTC epoch ms |
| `POST /devices/{deviceId}/commands` | Control a device; owner/admin |
| `PUT /devices/{deviceId}/settings` | Partial settings update; admin |
| `GET /devices/{deviceId}/commands` | Command history |
| `GET /commands/{commandId}` | Command status; owner/admin |

List endpoints support `limit` and `offset`. Commands use the following bodies:

```json
{"command":"SET_DOOR_ANGLE","value":90}
```

```json
{"command":"SET_PUMP","value":true}
```

```json
{"command":"RESET_SAFE"}
```

```json
{"command":"EMERGENCY","value":true}
```

`EMERGENCY` is admin-only and the UI must require explicit confirmation. `false` releases emergency mode. Logical door angle accepts integer values from 0–180; the UI may use 0/45/90/180. Firmware limits manual override to 10 seconds. An ACK does not prove physical door position, and the current firmware does not report actual pump state.

Settings are sent inside an `UPDATE_SETTINGS` command:

```json
PUT /devices/gate-001/settings
{"warningCm":10,"dangerCm":25,"criticalCm":40,"telemetryIntervalSec":5}
```

Every field is optional for a partial update. Limits are `0 <= warning < danger < critical` and an interval of 1–3600 seconds. Defaults are 10/25/40 cm and 5 seconds, assuming factory firmware defaults. SQL settings change only after a successful ACK. Further settings changes are blocked while a prior settings command is pending or timed out, preventing a late ACK from reversing configuration order.

HTTP 202 means the command was stored and submitted to MQTT. Monitor `PENDING → SUCCESS / FAILED / TIMEOUT`. The default timeout is 10 seconds and means the final execution result is unknown. There is no automatic actuator retry, although late ACKs are still accepted.

## Data behavior

- Subscribes to `iot/+/+/telemetry`, `iot/+/+/ack`, and `iot/+/+/status`; publishes QoS 0, non-retained commands to `iot/{region}/{deviceId}/cmd`.
- A device timestamp before 01 January 2023 or more than five minutes in the future is replaced with server receive time. API timestamps are UTC epoch milliseconds.
- Backend classification uses active thresholds: CRITICAL, DANGER, WARNING for water contact or warning level, otherwise SAFE. Firmware's compact status never overrides this calculation.
- Temperature `-1`, voltage `0`, and the infrared placeholder are treated as N/A. `doorAngle` is the firmware's logical position.
- Rainfall is Open-Meteo `rain + showers` from the most recently available hour, reported in mm/hour. It is cached for 10 minutes per coordinate and is not a physical rain sensor. API failures return `null`, never `-1` or zero.
- An LWT offline event is applied immediately. Retained online status must contain a fresh epoch timestamp. A device becomes offline after 60 seconds without a live message.
- Older telemetry remains in history without replacing a newer snapshot. Malformed MQTT payloads are discarded and logged.

## MySQL and Docker

For local MySQL outside Docker, create a database and user, then set `DATABASE_URL=mysql+pymysql://USER:PASSWORD@localhost:3306/aquashield`. URL-encode special characters in passwords. Tables are created on first startup; schema changes require planned migrations because Alembic is not configured.

For Docker, fill `MYSQL_PASSWORD` and `MYSQL_ROOT_PASSWORD` in `.env` with different random alphanumeric values, then run:

```powershell
docker compose up --build -d
docker compose ps
Invoke-RestMethod http://localhost:8000/health
```

During the first MySQL boot, the API waits for the database to become ready. Expected health output when all backend integrations are connected:

```json
{"status":"ok","mqttConnected":true,"firebaseMirrorEnabled":true}
```

`mqttConnected: true` proves only that the backend connected to the broker. A newly registered device becomes `isOnline: true` only after its ESP32 sends a matching status or telemetry message. MySQL data is stored in a Docker volume and Firebase credentials are mounted read-only. Run **one worker / one replica** because MQTT subscription and command coordination currently live in the API process.

## Tests and limits

```powershell
.\.venv\Scripts\python -m pytest -q --basetemp .test-runs\manual-check
```

Tests use temporary SQLite and mocked external services for classification, timestamps, enrichment, user/admin access, command ACK/timeout, retained/LWT behavior, mirror retry, weather, and MQTT TCP/WebSocket contracts. Team MySQL, Firebase, the real broker, and physical actuators still need end-to-end testing. Before sending Swagger commands, ensure the device is safe and operator approval is in place because commands can move actuators. The ingestion queue is bounded and in memory, so unpersisted data can be lost if the process stops. History has no automated retention policy.

References: [Firebase Admin setup](https://firebase.google.com/docs/admin/setup), [verify ID tokens](https://firebase.google.com/docs/auth/admin/verify-id-tokens), [Paho MQTT](https://eclipse.dev/paho/files/paho.mqtt.python/html/client.html), [Open-Meteo](https://open-meteo.com/en/docs).
