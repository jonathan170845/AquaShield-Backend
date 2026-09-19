from unittest.mock import Mock, patch
import httpx
from app.integrations import Weather, MQTT
from app.config import Settings
from app.models import now_ms


def test_weather_hourly_rain_and_showers_cached():
    weather = Weather(600)
    weather.client.close()
    weather.client = Mock()
    weather.client.get.return_value.json.return_value = {'hourly': {
        'time': [now_ms() // 1000 - 60], 'rain': [1.2], 'showers': [0.8]}}
    result = weather.rainfall(-6.2, 106.8)
    assert result['rainfallMmHr'] == 2
    assert weather.rainfall(-6.2, 106.8) == result
    assert weather.client.get.call_count == 1


def test_weather_failure_returns_na_and_is_cached():
    weather = Weather(600)
    weather.client.close()
    weather.client = Mock()
    weather.client.get.side_effect = httpx.ConnectError('offline')
    assert weather.rainfall(0, 0)['rainfallMmHr'] is None
    assert weather.rainfall(0, 0)['rainfallSource'] == 'unavailable'
    assert weather.client.get.call_count == 1


def test_mqtt_contract():
    mqtt = MQTT(Settings(_env_file=None))
    client = Mock()
    mqtt.on_connect(client, None, None, Mock(is_failure=False), None)
    client.subscribe.assert_called_once_with([
        ('iot/+/+/telemetry',1), ('iot/+/+/ack',1), ('iot/+/+/status',1)])
    mqtt.client = client
    client.publish.return_value.rc = 0
    mqtt.publish('iot/r/d/cmd', {'command':'RESET_SAFE'})
    assert client.publish.call_args.kwargs == {'qos':0, 'retain':False}
    message = Mock(topic='iot/r/d/status', payload=b'{"isOnline":false}', retain=True)
    mqtt.on_message(client, None, message)
    queued = mqtt.messages.get_nowait()
    assert queued[:3] == (message.topic, message.payload, True)


def test_mqtt_websocket_transport():
    client = Mock()
    settings = Settings(_env_file=None, mqtt_transport='websockets', mqtt_websocket_path='/broker', mqtt_tls=True)
    with patch('app.integrations.mqtt.Client', return_value=client) as client_class:
        MQTT(settings)
    assert client_class.call_args.kwargs['transport'] == 'websockets'
    client.tls_set.assert_called_once_with()
    client.ws_set_options.assert_called_once_with(path='/broker')
