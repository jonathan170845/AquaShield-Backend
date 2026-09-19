from typing import Literal
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')
    database_url: str = 'sqlite:///./aquashield.db'
    auth_mode: Literal['firebase', 'development'] = 'firebase'
    dev_api_token: str = ''
    firebase_enabled: bool = False
    firebase_credentials_path: str = 'secrets/service-account.json'
    firebase_database_url: str = ''
    mqtt_enabled: bool = False
    mqtt_host: str = 'mqtt.bifriends.my.id'
    mqtt_port: int = 1883
    mqtt_username: str = ''
    mqtt_password: str = ''
    mqtt_tls: bool = False
    mqtt_transport: Literal['tcp', 'websockets'] = 'tcp'
    mqtt_websocket_path: str = '/mqtt'
    command_timeout_seconds: int = Field(default=10, ge=1)
    offline_after_seconds: int = Field(default=60, ge=20)
    weather_cache_seconds: int = Field(default=600, ge=60)
    cors_origins: list[str] = []

    @model_validator(mode='after')
    def validate_development(self):
        if self.auth_mode == 'development' and len(self.dev_api_token) < 24:
            raise ValueError('Development mode requires DEV_API_TOKEN of at least 24 characters')
        if self.firebase_enabled and not self.firebase_database_url:
            raise ValueError('FIREBASE_DATABASE_URL is required for mirroring')
        return self
