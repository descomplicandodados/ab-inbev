import pytest
from unittest.mock import patch, MagicMock
from brewery_pipeline.ingest import Brewery, fetch_breweries

@pytest.fixture
def sample_record():
    return {
        "id": "1",
        "name": "Test",
        "brewery_type": "micro",
        "address_1": None,
        "address_2": None,
        "address_3": None,
        "city": "City",
        "state": "State",
        "state_province": "Province",
        "postal_code": "00000",
        "country": "Country",
        "longitude": "1.23",
        "latitude": "4.56",
        "phone": "123",
        "website_url": "http://test.com",
        "street": "Street",
    }

def test_pydantic_model(sample_record):
    brewery = Brewery(**sample_record)
    assert brewery.id == "1"
    assert isinstance(brewery.longitude, str)

@patch("brewery_pipeline.ingest.requests.get")
def test_fetch_breweries(mock_get, sample_record):
    mock_get.return_value.json.side_effect = [[sample_record], []]
    mock_get.return_value.raise_for_status = MagicMock()

    result = list(fetch_breweries())
    assert len(result) == 1
    assert result[0][0]["id"] == "1"