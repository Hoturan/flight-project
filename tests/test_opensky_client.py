"""Tests for the OpenSky HTTP client (uses mocked responses)."""

from unittest.mock import patch, MagicMock

from src.utils.opensky_client import OpenSkyClient


MOCK_PAYLOAD = {
    "time": 1753097600,
    "states": [
        ["3c6444", "DLH123  ", "Germany", 1753097598.0, 6.1543,
         50.0371, 7.1234, 11277.6, False, 232.5, 90.1, 5.2,
         None, 11450.6, "1000", False, 0],
    ],
}


def test_fetch_states_success():
    """Client should return parsed JSON on a 200 response."""
    client = OpenSkyClient(retries=1)

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = MOCK_PAYLOAD
    mock_resp.raise_for_status.return_value = None

    with patch("src.utils.opensky_client.requests.get", return_value=mock_resp):
        result = client.fetch_states()

    assert result["time"] == 1753097600
    assert len(result["states"]) == 1


def test_flatten_states():
    """flatten_states should convert array-of-arrays to list of dicts."""
    rows = OpenSkyClient.flatten_states(MOCK_PAYLOAD)

    assert len(rows) == 1
    assert rows[0]["icao24"] == "3c6444"
    assert rows[0]["callsign"] == "DLH123  "
    assert rows[0]["origin_country"] == "Germany"


def test_flatten_states_empty():
    """flatten_states should handle missing states key gracefully."""
    rows = OpenSkyClient.flatten_states({"time": 123, "states": None})
    assert rows == []