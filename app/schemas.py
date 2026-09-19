from typing import Annotated, Literal
from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, model_validator

Identifier = Annotated[str, Field(pattern=r'^[a-zA-Z0-9_-]{1,80}$')]


class Payload(BaseModel):
    model_config = ConfigDict(extra='forbid', allow_inf_nan=False)


class DeviceCreate(Payload):
    deviceId: Identifier
    region: Identifier
    name: str = Field(min_length=1, max_length=120)
    ownerUid: str = Field(min_length=1, max_length=128, pattern=r'^[^.$\[\]#/\x00-\x1f\x7f]+$')
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)


class DeviceSettings(Payload):
    warningCm: float | None = Field(default=None, ge=0)
    dangerCm: float | None = Field(default=None, ge=0)
    criticalCm: float | None = Field(default=None, ge=0)
    telemetryIntervalSec: int | None = Field(default=None, ge=1, le=3600)

    @model_validator(mode='after')
    def nonempty(self):
        if not self.model_fields_set or any(getattr(self, k) is None for k in self.model_fields_set):
            raise ValueError('Supply at least one non-null setting')
        return self


class CommandRequest(Payload):
    command: Literal['SET_DOOR_ANGLE', 'SET_PUMP', 'RESET_SAFE', 'EMERGENCY', 'UPDATE_SETTINGS']
    value: StrictBool | StrictInt | DeviceSettings | None = None

    @model_validator(mode='after')
    def valid_value(self):
        if self.command == 'SET_DOOR_ANGLE' and (type(self.value) is not int or not 0 <= self.value <= 180):
            raise ValueError('Door angle must be an integer from 0 to 180')
        if self.command in ('SET_PUMP', 'EMERGENCY') and type(self.value) is not bool:
            raise ValueError('This command requires a boolean value')
        if self.command == 'RESET_SAFE' and self.value is not None:
            raise ValueError('RESET_SAFE takes no value')
        if self.command == 'UPDATE_SETTINGS' and not isinstance(self.value, DeviceSettings):
            raise ValueError('UPDATE_SETTINGS requires a settings object')
        return self


class TelemetryIn(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)
    deviceId: Identifier
    waterLevelCm: float = Field(ge=0)
    waterDetected: StrictBool
    rainfallMmHr: float = -1
    flowRateLMin: float = Field(ge=0)
    temperatureC: float = -1
    rssiDbm: int
    voltage: float = 0
    infraredStatus: str = 'CLEAR'
    doorAngle: int = Field(ge=0, le=180)
    timestamp: StrictInt


class AckIn(BaseModel):
    commandId: str = Field(max_length=36)
    deviceId: Identifier
    status: Literal['SUCCESS', 'FAILED']
    message: str = Field(default='', max_length=1000)
    timestamp: StrictInt | None = None


class StatusIn(BaseModel):
    deviceId: Identifier
    isOnline: StrictBool
    floodStatus: Literal['SAFE', 'WARNING', 'DANGER', 'CRITICAL'] | None = None
    timestamp: StrictInt | None = None
