"""YAML job configuration loading helpers."""

from __future__ import annotations

from pathlib import Path
from typing import TypeVar

import yaml  # type: ignore[import-untyped]
from pydantic import BaseModel, ValidationError

from ctx_ctr.exceptions import CtxCtrError

ConfigModel = TypeVar("ConfigModel", bound=BaseModel)


class JobConfigError(CtxCtrError):
    """Raised when a job configuration file cannot be loaded or validated."""


def load_job_config(config_path: str, model_type: type[ConfigModel]) -> ConfigModel:
    """Load a YAML job configuration into the requested Pydantic model."""

    path = Path(config_path)
    try:
        raw_config = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise JobConfigError(f"Failed to read job config {path}") from error
    except yaml.YAMLError as error:
        raise JobConfigError(f"Failed to parse job config {path}") from error

    if raw_config is None:
        raw_config = {}
    if not isinstance(raw_config, dict):
        raise JobConfigError(f"Job config {path} must contain a YAML object")

    try:
        return model_type.model_validate(raw_config)
    except ValidationError as error:
        raise JobConfigError(f"Job config {path} is invalid") from error


def merge_job_config(config: ConfigModel, overrides: dict[str, object]) -> ConfigModel:
    """Apply CLI overrides to a loaded job config and re-validate the result."""

    return type(config).model_validate({**config.model_dump(), **overrides})
