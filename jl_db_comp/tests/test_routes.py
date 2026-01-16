import json

import pytest
from tornado.httpclient import HTTPClientError


async def test_completions_no_db_url(jp_fetch):
    """Test completions endpoint without database URL returns empty results."""
    # When - fetch completions without db_url parameter
    response = await jp_fetch("jl-db-comp", "completions")

    # Then
    assert response.code == 200
    payload = json.loads(response.body)
    assert payload["status"] == "success"
    assert payload["tables"] == []
    assert payload["columns"] == []
    assert "No connection specified" in payload.get("message", "")


async def test_completions_with_invalid_db_url(jp_fetch):
    """Test completions endpoint with invalid database URL handles errors gracefully."""
    # When - fetch completions with invalid db_url
    with pytest.raises(HTTPClientError) as exc_info:
        await jp_fetch(
            "jl-db-comp", "completions", params={"db_url": "postgresql://invalid:url"}
        )

    # Then - should return 500 error with error message
    assert exc_info.value.code == 500
    payload = json.loads(exc_info.value.response.body)
    assert payload["status"] == "error"
    assert "message" in payload


async def test_completions_schema_parameter(jp_fetch):
    """Test completions endpoint accepts schema parameter."""
    # When - fetch completions with schema parameter
    response = await jp_fetch(
        "jl-db-comp", "completions", params={"schema": "public", "prefix": "test"}
    )

    # Then
    assert response.code == 200
    payload = json.loads(response.body)
    assert payload["status"] == "success"
    # Without a valid database, should return empty results
    assert isinstance(payload["tables"], list)
    assert isinstance(payload["columns"], list)
