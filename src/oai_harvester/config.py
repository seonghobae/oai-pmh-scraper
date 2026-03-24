from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence


def _env_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean value: {value!r}")


def _env_text(value: str | None, default: str) -> str:
    return (value or default).strip()


def _env_optional_text(value: str | None) -> str | None:
    return (value or "").strip() or None


def _split_terms(value: str | None, default_terms: Sequence[str]) -> tuple[str, ...]:
    if not value:
        return tuple(default_terms)
    terms = tuple(term.strip().lower() for term in value.split(",") if term.strip())
    return terms if terms else tuple(default_terms)


@dataclass(frozen=True)
class HarvesterConfig:
    base_url: str
    metadata_prefix: str
    set_spec: str | None
    from_date: str | None
    until_date: str | None
    state_file: Path
    open_access_only: bool
    open_access_terms: tuple[str, ...]
    batch_size: int
    timeout_seconds: int
    user_agent: str

    # Snowflake
    sf_account: str | None = None
    sf_user: str | None = None
    sf_password: str | None = None
    sf_role: str | None = None
    sf_warehouse: str | None = None
    sf_database: str = "HARMONIA"
    sf_schema: str = "PUBLIC"
    sf_table: str = "PAPERS"

    @property
    def is_snowflake_enabled(self) -> bool:
        return (
            self.sf_account is not None
            and self.sf_user is not None
            and self.sf_password is not None
        )


def load_config(env: Mapping[str, str | None]) -> HarvesterConfig:
    base_url = _env_text(env.get("OAI_BASE_URL"), "")
    if not base_url:
        raise ValueError("OAI_BASE_URL is required")

    metadata_prefix = _env_text(env.get("OAI_METADATA_PREFIX"), "oai_dc")
    if not metadata_prefix:
        raise ValueError("OAI_METADATA_PREFIX must not be empty")

    set_spec = _env_text(env.get("OAI_SET"), "") or None
    from_date = _env_text(env.get("OAI_FROM"), "") or None
    until_date = _env_text(env.get("OAI_UNTIL"), "") or None
    state_file = Path(
        _env_text(env.get("OAI_STATE_FILE"), ".oai_harvest_state.json")
    ).expanduser()

    open_access_only = _env_bool(env.get("OPEN_ACCESS_ONLY"), False)
    default_terms = (
        "open access",
        "open_access",
        "creative commons",
        "cc-by",
        "cc 0",
        "cc0",
        "gold",
    )
    open_access_terms = _split_terms(env.get("OPEN_ACCESS_TERMS"), default_terms)

    try:
        batch_size = int(_env_text(env.get("HARVEST_BATCH_SIZE"), "500"))
    except ValueError as exc:
        raise ValueError("HARVEST_BATCH_SIZE must be integer") from exc
    if batch_size < 1:
        raise ValueError("HARVEST_BATCH_SIZE must be >= 1")

    try:
        timeout_seconds = int(_env_text(env.get("OAI_REQUEST_TIMEOUT"), "30"))
    except ValueError as exc:
        raise ValueError("OAI_REQUEST_TIMEOUT must be integer") from exc
    if timeout_seconds < 1:
        raise ValueError("OAI_REQUEST_TIMEOUT must be >= 1")

    user_agent = _env_text(env.get("OAI_USER_AGENT"), "oai-pmh-scraper/0.1.0")

    sf_account = _env_optional_text(env.get("SNOWFLAKE_ACCOUNT"))
    sf_user = _env_optional_text(env.get("SNOWFLAKE_USER"))
    sf_password = _env_optional_text(env.get("SNOWFLAKE_PASSWORD"))
    required_snowflake = (sf_account, sf_user, sf_password)
    if any(required_snowflake) and not all(required_snowflake):
        raise ValueError(
            "SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, and SNOWFLAKE_PASSWORD must be set together"
        )

    return HarvesterConfig(
        base_url=base_url,
        metadata_prefix=metadata_prefix,
        set_spec=set_spec,
        from_date=from_date,
        until_date=until_date,
        state_file=state_file,
        open_access_only=open_access_only,
        open_access_terms=open_access_terms,
        batch_size=batch_size,
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
        sf_account=sf_account,
        sf_user=sf_user,
        sf_password=sf_password,
        sf_role=_env_optional_text(env.get("SNOWFLAKE_ROLE")),
        sf_warehouse=_env_optional_text(env.get("SNOWFLAKE_WAREHOUSE")),
        sf_database=_env_text(env.get("SNOWFLAKE_DATABASE"), "HARMONIA"),
        sf_schema=_env_text(env.get("SNOWFLAKE_SCHEMA"), "PUBLIC"),
        sf_table=_env_text(env.get("SNOWFLAKE_TABLE"), "PAPERS"),
    )
