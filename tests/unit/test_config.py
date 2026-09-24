import pytest

from pipeline import db
from pipeline.config import get_settings

BASE_ENV = {
    "S3_BUCKET": "bucket",
    "PGHOST": "db",
    "PGUSER": "user",
    "PGPASSWORD": "password",
    "PGDATABASE": "warehouse",
}


@pytest.fixture
def env(monkeypatch):
    for name in ("S3_ENDPOINT", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "PGSSLMODE", "PGPORT"):
        monkeypatch.delenv(name, raising=False)
    for name, value in BASE_ENV.items():
        monkeypatch.setenv(name, value)
    return monkeypatch


def test_without_keys_the_default_credential_chain_is_used(env):
    settings = get_settings()
    assert settings.aws_access_key_id is None
    assert settings.aws_secret_access_key is None


def test_explicit_keys_are_used(env):
    env.setenv("AWS_ACCESS_KEY_ID", "key-id")
    env.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    settings = get_settings()
    assert (settings.aws_access_key_id, settings.aws_secret_access_key) == ("key-id", "secret")


def test_half_a_key_pair_fails_loudly(env):
    env.setenv("AWS_ACCESS_KEY_ID", "key-id")
    with pytest.raises(RuntimeError, match="both"):
        get_settings()


def test_sslmode_reaches_the_jdbc_url(env):
    assert get_settings().jdbc_url == "jdbc:postgresql://db:5432/warehouse"
    env.setenv("PGSSLMODE", "require")
    assert get_settings().jdbc_url == "jdbc:postgresql://db:5432/warehouse?sslmode=require"


def test_database_connections_always_set_sslmode(env):
    # libpq rejects an empty PGSSLMODE variable, so connect() must never fall back to it.
    calls = []
    env.setattr(db.psycopg, "connect", lambda **kwargs: calls.append(kwargs))
    env.setenv("PGSSLMODE", "")
    db.connect(get_settings())
    env.setenv("PGSSLMODE", "require")
    db.connect(get_settings())
    assert [call["sslmode"] for call in calls] == ["prefer", "require"]
