import json
from unittest.mock import Mock
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from app.config import Settings
from app.domain import event_timestamp, flood_status, normalize_telemetry
from app.main import create_app, identity
from app.models import Command, Device, Telemetry, database, now_ms
from app.service import Service


@pytest.fixture
def system(tmp_path):
    cfg = Settings(_env_file=None, database_url=f'sqlite:///{tmp_path / "test.db"}',
                   auth_mode='development', dev_api_token='a-local-test-token-at-least-24')
    engine, sessions = database(cfg.database_url)
    mqtt = Mock(connected=True)
    weather = Mock()
    weather.rainfall.return_value = {'rainfallMmHr': 2.5, 'rainfallSource': 'test', 'rainfallObservedAt': now_ms()}
    firebase = Mock(enabled=True)
    svc = Service(sessions, cfg, mqtt, weather, firebase)
    with sessions() as db:
        db.add(Device(id='gate-001', region='jakarta-barat', name='Home', owner_uid='owner', latitude=-6.2, longitude=106.8))
        db.commit()
    app = create_app(cfg, svc)
    app.state.settings, app.state.service = cfg, svc
    app.dependency_overrides[identity] = lambda: {'uid': 'owner', 'admin': True}
    # No lifespan: tests exercise workers deterministically, without network or background races.
    client = TestClient(app)
    yield svc, client, app
    client.close()
    engine.dispose()


def ingest(svc, channel, data, retained=False, received=None):
    svc.ingest(f'iot/jakarta-barat/gate-001/{channel}', json.dumps({'deviceId': 'gate-001', **data}), retained, received)


def telemetry(**extra):
    return {'waterLevelCm': 26, 'waterDetected': False, 'rainfallMmHr': -1,
            'flowRateLMin': 0, 'temperatureC': -1, 'rssiDbm': -55,
            'voltage': 0, 'infraredStatus': 'CLEAR', 'doorAngle': 90, 'timestamp': 1234, **extra}


@pytest.mark.parametrize('level,wet,expected', [(0,False,'SAFE'), (0,True,'WARNING'),
    (10,False,'WARNING'), (25,False,'DANGER'), (40,False,'CRITICAL')])
def test_classification(level, wet, expected):
    assert flood_status(wet, level, {'warningCm':10,'dangerCm':25,'criticalCm':40}) == expected


def test_timestamp():
    now = now_ms()
    assert event_timestamp(1234, now) == (now, 'server')
    assert event_timestamp(now + 900000, now) == (now, 'server')
    assert event_timestamp(now - 1000, now) == (now - 1000, 'device')


def test_telemetry_normalization_keeps_future_sensor_values():
    payload = telemetry(temperatureC=28.5, voltage=3.7, infraredStatus='BLOCKED')
    normalized = normalize_telemetry(payload, {'rainfallMmHr': 1.5}, now_ms(), 'device', now_ms(),
                                    {'warningCm': 10, 'dangerCm': 25, 'criticalCm': 40})
    assert normalized['temperatureC'] == 28.5
    assert normalized['voltage'] == 3.7
    assert normalized['infraredStatus'] == 'BLOCKED'


def test_malformed_mqtt_payload_is_discarded(system):
    svc, client, _ = system
    svc.ingest('iot/jakarta-barat/gate-001/telemetry', b'not-json')
    assert client.get('/devices/gate-001/telemetry').json() == []


def test_telemetry_enrichment_and_history(system):
    svc, client, _ = system
    received = now_ms()
    ingest(svc, 'telemetry', telemetry(), received=received)
    latest = client.get('/devices/gate-001').json()['latest']
    assert latest['rainfallMmHr'] == 2.5
    assert latest['temperatureC'] is None and latest['voltage'] is None
    assert latest['infraredStatus'] is None
    assert latest['floodStatus'] == 'DANGER'
    assert latest['timestamp'] == received and latest['timestampSource'] == 'server'
    assert len(client.get('/devices/gate-001/telemetry').json()) == 1
    svc.mirror()
    assert svc.firebase.write.call_count == 1


def test_settings_commit_only_on_success_and_late_ack(system):
    svc, client, _ = system
    response = client.put('/devices/gate-001/settings', json={'warningCm': 12})
    assert response.status_code == 202
    command = response.json()
    assert command['value'] == {'warningCm': 12}
    assert client.get('/devices/gate-001').json()['settings']['warningCm'] == 10
    assert client.put('/devices/gate-001/settings', json={'warningCm': 13}).status_code == 409
    with svc.sessions() as db:
        c = db.get(Command, command['commandId'])
        c.created_ms = now_ms() - 20000
        db.commit()
    svc.maintain()
    assert client.get('/commands/' + command['commandId']).json()['status'] == 'TIMEOUT'
    ingest(svc, 'ack', {'commandId': command['commandId'], 'status': 'SUCCESS', 'timestamp': 123})
    assert client.get('/devices/gate-001').json()['settings']['warningCm'] == 12
    ingest(svc, 'ack', {'commandId': command['commandId'], 'status': 'FAILED'})
    assert client.get('/commands/' + command['commandId']).json()['status'] == 'SUCCESS'


def test_failed_settings_and_invalid_merged_thresholds(system):
    svc, client, _ = system
    assert client.put('/devices/gate-001/settings', json={'warningCm': 30}).status_code == 422
    c = client.put('/devices/gate-001/settings', json={'warningCm': 11}).json()
    ingest(svc, 'ack', {'commandId': c['commandId'], 'status': 'FAILED'})
    assert client.get('/devices/gate-001').json()['settings']['warningCm'] == 10


def test_status_retained_lwt_and_staleness(system):
    svc, client, _ = system
    ingest(svc, 'status', {'isOnline': True, 'timestamp': now_ms() - 900000}, retained=True)
    assert client.get('/devices/gate-001').json()['isOnline'] is False
    ingest(svc, 'status', {'isOnline': True, 'timestamp': now_ms()})
    assert client.get('/devices/gate-001').json()['isOnline'] is True
    ingest(svc, 'status', {'isOnline': False})
    assert client.get('/devices/gate-001').json()['isOnline'] is False
    ingest(svc, 'status', {'isOnline': True}, received=now_ms() - 900000)
    svc.maintain()
    assert client.get('/devices/gate-001').json()['isOnline'] is False


def test_permissions_and_auth(system):
    svc, client, app = system
    app.dependency_overrides[identity] = lambda: {'uid': 'other'}
    assert client.get('/devices').json() == []
    assert client.get('/devices/gate-001').status_code == 404
    assert client.post('/devices/gate-001/commands', json={'command':'SET_PUMP','value':True}).status_code == 404
    app.dependency_overrides[identity] = lambda: {'uid': 'owner'}
    assert client.post('/devices/gate-001/commands', json={'command':'EMERGENCY','value':True}).status_code == 403
    assert client.put('/devices/gate-001/settings', json={'warningCm': 11}).status_code == 403
    assert client.post('/devices/gate-001/commands', json={'command':'SET_PUMP','value':True}).status_code == 202
    app.dependency_overrides.clear()
    assert client.get('/devices').status_code == 401
    assert client.get('/devices', headers={'Authorization':'Bearer invalid'}).status_code == 401
    assert client.get('/devices', headers={'Authorization':'Bearer ' + svc.settings.dev_api_token}).status_code == 200


@pytest.mark.parametrize('body', [{'command':'SET_DOOR_ANGLE','value':True},
    {'command':'SET_DOOR_ANGLE','value':181}, {'command':'SET_PUMP','value':1},
    {'command':'EMERGENCY','value':'true'}, {'command':'RESET_SAFE','value':False},
    {'command':'UPDATE_SETTINGS','value':{}}, {'command':'UPDATE_SETTINGS','value':{'warningCm':None}}])
def test_invalid_commands(system, body):
    _, client, _ = system
    assert client.post('/devices/gate-001/commands', json=body).status_code == 422


def test_publish_failure_and_disconnected(system):
    svc, client, _ = system
    svc.mqtt.connected = False
    assert client.post('/devices/gate-001/commands', json={'command':'RESET_SAFE'}).status_code == 503
    svc.mqtt.connected = True
    svc.mqtt.publish.side_effect = RuntimeError()
    assert client.post('/devices/gate-001/commands', json={'command':'RESET_SAFE'}).status_code == 503
    with svc.sessions() as db:
        assert db.scalar(select(Command)).status == 'FAILED'


def test_mirror_retry(system):
    svc, _, _ = system
    svc.firebase.write.side_effect = RuntimeError()
    with pytest.raises(RuntimeError):
        svc.mirror()
    with svc.sessions() as db:
        assert db.get(Device, 'gate-001').mirror_dirty
    svc.firebase.write.side_effect = None
    svc.mirror()
    with svc.sessions() as db:
        assert not db.get(Device, 'gate-001').mirror_dirty


def test_topic_mismatch_and_old_telemetry(system):
    svc, client, _ = system
    svc.ingest('iot/wrong/gate-001/telemetry', json.dumps({'deviceId':'gate-001', **telemetry()}))
    with svc.sessions() as db:
        assert db.scalar(select(Telemetry)) is None
    t = now_ms()
    ingest(svc, 'telemetry', telemetry(timestamp=t, waterLevelCm=40))
    ingest(svc, 'telemetry', telemetry(timestamp=t-10000, waterLevelCm=0))
    assert client.get('/devices/gate-001').json()['latest']['floodStatus'] == 'CRITICAL'


def test_real_application_startup_shutdown(tmp_path):
    cfg = Settings(_env_file=None, database_url=f'sqlite:///{tmp_path / "startup.db"}',
                   auth_mode='development', dev_api_token='a-local-test-token-at-least-24')
    with TestClient(create_app(cfg)) as client:
        assert client.get('/health').json() == {
            'status':'ok', 'mqttConnected':False, 'firebaseMirrorEnabled':False}
        assert client.get('/openapi.json').status_code == 200
        assert client.post('/devices', headers={'Authorization':'Bearer ' + cfg.dev_api_token},
            json={'deviceId':'test', 'region':'jakarta', 'name':'Test', 'ownerUid':'local-admin',
                  'latitude':-6.2, 'longitude':106.8}).status_code == 201


def published(svc):
    topic, payload = svc.mqtt.publish.call_args.args
    return topic, json.loads(json.dumps(payload))


def test_command_wire_format_matches_firmware(system):
    svc, client, _ = system
    client.post('/devices/gate-001/commands', json={'command': 'SET_PUMP', 'value': True})
    topic, payload = published(svc)
    assert topic == 'iot/jakarta-barat/gate-001/cmd'
    assert set(payload) == {'commandId', 'command', 'value'} and payload['value'] is True
    client.post('/devices/gate-001/commands', json={'command': 'SET_PUMP', 'value': False})
    assert published(svc)[1]['value'] is False
    client.post('/devices/gate-001/commands', json={'command': 'RESET_SAFE'})
    assert 'value' not in published(svc)[1]
    client.post('/devices/gate-001/commands', json={'command': 'SET_DOOR_ANGLE', 'value': 0})
    assert published(svc)[1]['value'] == 0


def test_update_settings_never_sends_nulls_and_fits_firmware_buffer(system):
    svc, client, _ = system
    assert client.put('/devices/gate-001/settings', json={'warningCm': 15, 'dangerCm': None}).status_code == 422
    assert client.put('/devices/gate-001/settings', json={'warningCm': 15}).status_code == 202
    _, payload = published(svc)
    assert payload['value'] == {'warningCm': 15} and None not in payload['value'].values()
    full = {'commandId': 'x' * 36, 'command': 'UPDATE_SETTINGS',
            'value': {'warningCm': 10.5, 'dangerCm': 25.5, 'criticalCm': 40.5, 'telemetryIntervalSec': 3600}}
    assert len(json.dumps(full, separators=(',', ':'))) < 200


def test_timed_out_settings_command_does_not_lock_settings(system):
    svc, client, _ = system
    first = client.put('/devices/gate-001/settings', json={'warningCm': 12}).json()
    with svc.sessions() as db:
        db.get(Command, first['commandId']).created_ms = now_ms() - 20000
        db.commit()
    svc.maintain()
    second = client.put('/devices/gate-001/settings', json={'warningCm': 14})
    assert second.status_code == 202
    ingest(svc, 'ack', {'commandId': second.json()['commandId'], 'status': 'SUCCESS', 'timestamp': 1})
    ingest(svc, 'ack', {'commandId': first['commandId'], 'status': 'SUCCESS', 'timestamp': 1})
    assert client.get('/devices/gate-001').json()['settings']['warningCm'] == 14


def test_firmware_telemetry_before_ntp_sync_and_lwt(system):
    svc, client, _ = system
    received = now_ms()
    ingest(svc, 'telemetry', telemetry(timestamp=48213), received=received)
    latest = client.get('/devices/gate-001').json()['latest']
    assert latest['timestamp'] == received and latest['timestampSource'] == 'server'
    assert latest['rainfallMmHr'] == 2.5 and latest['infraredStatus'] is None
    svc.ingest('iot/jakarta-barat/gate-001/status', '{"deviceId":"gate-001","isOnline":false}', True)
    assert client.get('/devices/gate-001').json()['isOnline'] is False


def test_unregistered_or_wrong_region_is_ignored(system):
    svc, client, _ = system
    svc.ingest('iot/jakarta-timur/gate-001/telemetry', json.dumps({'deviceId': 'gate-001', **telemetry()}))
    svc.ingest('iot/jakarta-barat/gate-999/telemetry', json.dumps({'deviceId': 'gate-999', **telemetry()}))
    assert client.get('/devices/gate-001/telemetry').json() == []
