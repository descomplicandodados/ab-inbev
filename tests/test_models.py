from src.silver import BreweryModel
import pytest

def test_valid_model():
    data = {
        "id": "1",
        "latitude": "10.5",
        "longitude": "20.5"
    }

    model = BreweryModel(**data)

    assert model.id == "1"
    assert isinstance(model.latitude, float)


def test_empty_latitude():
    data = {
        "id": "1",
        "latitude": "",
        "longitude": ""
    }

    model = BreweryModel(**data)

    assert model.latitude is None
    assert model.longitude is None


def test_invalid_latitude():
    data = {
        "id": "1",
        "latitude": "999"
    }

    with pytest.raises(Exception):
        BreweryModel(**data)