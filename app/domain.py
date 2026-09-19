NTP_SYNCED_THRESHOLD_MS = 1_672_531_200_000
MAX_FUTURE_TIMESTAMP_DRIFT_MS = 5 * 60 * 1000


def event_timestamp(timestamp, received_ms):
    if (timestamp is not None
            and NTP_SYNCED_THRESHOLD_MS <= timestamp <= received_ms + MAX_FUTURE_TIMESTAMP_DRIFT_MS):
        return timestamp, 'device'
    return received_ms, 'server'


def flood_status(water_detected, level_cm, settings):
    if level_cm >= settings['criticalCm']:
        return 'CRITICAL'
    if level_cm >= settings['dangerCm']:
        return 'DANGER'
    if water_detected or level_cm >= settings['warningCm']:
        return 'WARNING'
    return 'SAFE'


def normalize_telemetry(payload, weather, timestamp, timestamp_source, received_ms, settings):
    """Prepare the single telemetry representation used by SQL, REST, and Firebase."""
    return {
        **payload,
        **weather,
        # These are firmware placeholders until their physical sensors are installed.
        'temperatureC': None if payload['temperatureC'] == -1 else payload['temperatureC'],
        'voltage': None if payload['voltage'] == 0 else payload['voltage'],
        'infraredStatus': None if payload['infraredStatus'] == 'CLEAR' else payload['infraredStatus'],
        'timestamp': timestamp,
        'timestampSource': timestamp_source,
        'receivedAt': received_ms,
        'floodStatus': flood_status(payload['waterDetected'], payload['waterLevelCm'], settings),
    }


def device_view(d):
    return {'deviceId': d.id, 'region': d.region, 'name': d.name,
            'ownerUid': d.owner_uid, 'latitude': d.latitude, 'longitude': d.longitude,
            'settings': d.settings, 'isOnline': d.is_online,
            'lastSeenAt': d.last_seen_ms, 'latest': d.latest}


def command_view(c):
    return {**c.payload, 'deviceId': c.device_id, 'status': c.status,
            'createdAt': c.created_ms, 'completedAt': c.completed_ms, 'message': c.message}
