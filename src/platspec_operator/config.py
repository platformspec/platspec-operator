"""Configuration management for the Platspec Operator."""

import os
from pathlib import Path
from typing import Any, Dict, Optional, Union

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class KubernetesConfig(BaseSettings):
    namespace: Optional[str] = Field(default=None)


class LoggingConfig(BaseSettings):
    level: str = Field(default="INFO")
    format: str = Field(default="json")

    @field_validator("level")
    @classmethod
    def validate_level(cls, v: str) -> str:
        valid = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid:
            raise ValueError(f"Log level must be one of: {valid}")
        return v.upper()


class BlueprintConfig(BaseSettings):
    blueprint_dir: Path = Field(default=Path("../../blueprints"))
    cache_enabled: bool = Field(default=True)
    kcl_timeout: int = Field(default=30)

    @field_validator("blueprint_dir")
    @classmethod
    def resolve_dir(cls, v: Path) -> Path:
        return Path(v).resolve()


class OperatorConfig(BaseSettings):
    reconcile_interval: int = Field(default=300)
    max_workers: int = Field(default=4)
    finalizer_name: str = Field(default="platspec.io/finalizer")
    field_manager: str = Field(default="platspec-operator")


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="PLATSPEC_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="allow",
    )

    kubernetes: KubernetesConfig = Field(default_factory=KubernetesConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    blueprint: BlueprintConfig = Field(default_factory=BlueprintConfig)
    operator: OperatorConfig = Field(default_factory=OperatorConfig)
    dev_mode: bool = Field(default=False)
    debug: bool = Field(default=False)

    @classmethod
    def load(cls, config_file: Optional[Path] = None) -> "Config":
        config_data: Dict[str, Any] = {}

        if config_file and config_file.exists():
            import yaml

            with open(config_file) as f:
                file_data = yaml.safe_load(f)
                if file_data:
                    config_data.update(file_data)

        env_overrides = cls._load_from_env()
        for key, value in env_overrides.items():
            if isinstance(value, dict) and key in config_data:
                if isinstance(config_data[key], dict):
                    config_data[key].update(value)
                else:
                    config_data[key] = value
            else:
                config_data[key] = value

        return cls(**config_data)

    @classmethod
    def _load_from_env(cls) -> Dict[str, Any]:
        env_vars: Dict[str, Any] = {}
        prefix = "PLATSPEC_"
        for key, value in os.environ.items():
            if key.startswith(prefix):
                config_key = key[len(prefix):].lower()
                if "_" in config_key:
                    section, setting = config_key.split("_", 1)
                    if section not in env_vars:
                        env_vars[section] = {}
                    env_vars[section][setting] = cls._coerce(value)
                else:
                    env_vars[config_key] = cls._coerce(value)
        return env_vars

    @staticmethod
    def _coerce(value: str) -> Union[str, int, bool, float]:
        if value.lower() in ("true", "false"):
            return value.lower() == "true"
        if value.isdigit():
            return int(value)
        try:
            if "." in value:
                return float(value)
        except ValueError:
            pass
        return value

    def __str__(self) -> str:
        return f"Config(dev_mode={self.dev_mode}, debug={self.debug})"
