from __future__ import annotations

import pytest

from oai_harvester.config import load_config


def test_load_config_minimal_env() -> None:
    env = {
        "OAI_BASE_URL": "https://example.org/oai",
    }
    cfg = load_config(env)
    assert cfg.base_url == "https://example.org/oai"
    assert cfg.metadata_prefix == "oai_dc"
    assert cfg.set_spec is None


def test_open_access_terms_split_and_bool() -> None:
    env = {
        "OAI_BASE_URL": "https://example.org/oai",
        "OPEN_ACCESS_ONLY": "true",
        "OPEN_ACCESS_TERMS": "CC BY, creative commons, gold",
    }
    cfg = load_config(env)
    assert cfg.open_access_only is True
    assert cfg.open_access_terms == ("cc by", "creative commons", "gold")


def test_invalid_batch_size_raises() -> None:
    env = {
        "OAI_BASE_URL": "https://example.org/oai",
        "HARVEST_BATCH_SIZE": "not-int",
    }
    with pytest.raises(ValueError, match="HARVEST_BATCH_SIZE"):
        load_config(env)


def test_non_positive_batch_size_raises() -> None:
    env = {
        "OAI_BASE_URL": "https://example.org/oai",
        "HARVEST_BATCH_SIZE": "0",
    }
    with pytest.raises(ValueError, match="HARVEST_BATCH_SIZE"):
        load_config(env)


def test_non_positive_timeout_raises() -> None:
    env = {
        "OAI_BASE_URL": "https://example.org/oai",
        "OAI_REQUEST_TIMEOUT": "-1",
    }
    with pytest.raises(ValueError, match="OAI_REQUEST_TIMEOUT"):
        load_config(env)
