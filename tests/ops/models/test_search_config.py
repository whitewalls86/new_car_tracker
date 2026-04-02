import pytest
from pydantic import ValidationError
from ops.models import search_config as sc

@pytest.fixture
def valid_config_defaults():
    return {
        "makes": ['Honda'],
        "models": ['CR-V'],
        "zip": '77080',
        "radius_miles": 200,
        "scopes": ["national", "local"],
        "max_listings": 2000,
        "max_safety_pages": 30,
        "sort_order": "best_match_desc",
        "sort_rotation": ["best_match_desc"]
    }


def test_valid_zip_code(valid_config_defaults):
    config = sc.SearchConfigParams(**valid_config_defaults)
    assert config.zip == "77080" 


@pytest.mark.parametrize("invalid_zip", [
    "ABCDE",    # Letters
    "123",      # Too short
    "123456",   # Too long
    "12 45",    # Space
    "",         # Empty
])
def test_invalid_zip_code(valid_config_defaults, invalid_zip):
    with pytest.raises(ValidationError):
        sc.SearchConfigParams(**{**valid_config_defaults, "zip": invalid_zip})


def test_valid_make(valid_config_defaults):
    config = sc.SearchConfigParams(**valid_config_defaults)
    assert config.makes == ["Honda"]


@pytest.mark.parametrize("invalid_makes", [
    [""],           # Empty string
    ["  "],         # Whitespace only
    ["  ", "\t"],   # Multiple whitespace items
])
def test_invalid_makes_whitespace(valid_config_defaults, invalid_makes):
    with pytest.raises(ValidationError):
        sc.SearchConfigParams(**{**valid_config_defaults, "makes": invalid_makes})


def test_valid_model(valid_config_defaults):
    config = sc.SearchConfigParams(**valid_config_defaults)
    assert config.models == ["CR-V"]


@pytest.mark.parametrize("invalid_models", [
    [""],           # Empty string
    ["  "],         # Whitespace only
    ["  ", "\t"],   # Multiple whitespace items
])
def test_invalid_models(valid_config_defaults, invalid_models):
    with pytest.raises(ValidationError):
        sc.SearchConfigParams(**{**valid_config_defaults, "makes": invalid_models})


def test_valid_scope(valid_config_defaults):
    config = sc.SearchConfigParams(**valid_config_defaults)
    assert config.scopes == ["national", "local"]


@pytest.mark.parametrize("invalid_scope", [
    [""],                # Empty string
    [],                  # Empty List
    ["alphabet", 'mongoose'],         # Whitespace only
    ["local", "invalid"],   # Multiple whitespace items
])
def test_invalid_scopes(valid_config_defaults, invalid_scope):
    with pytest.raises(ValidationError):
        sc.SearchConfigParams(**{**valid_config_defaults, "scopes": invalid_scope})


def test_valid_radius(valid_config_defaults):
    config = sc.SearchConfigParams(**valid_config_defaults)
    assert config.radius_miles == 200


def test_valid_radius_one(valid_config_defaults):
    config = sc.SearchConfigParams(**{**valid_config_defaults, "radius_miles": 1})
    assert config.radius_miles == 1


def test_valid_radius_5000(valid_config_defaults):
    config = sc.SearchConfigParams(**{**valid_config_defaults, "radius_miles": 5000})
    assert config.radius_miles == 5000


@pytest.mark.parametrize("invalid_radius", [
    0,                # Too Low
    -99,              # Negative
    5001              # Too High
])
def test_invalid_raidus(valid_config_defaults, invalid_radius):
    with pytest.raises(ValidationError):
        sc.SearchConfigParams(**{**valid_config_defaults, "radius_miles": invalid_radius})


def test_valid_max_listings(valid_config_defaults):
    config = sc.SearchConfigParams(**valid_config_defaults)
    assert config.max_listings == 2000


def test_valid_max_listings_one(valid_config_defaults):
    config = sc.SearchConfigParams(**{**valid_config_defaults, "max_listings": 1})
    assert config.max_listings == 1


def test_valid_max_listings_10000(valid_config_defaults):
    config = sc.SearchConfigParams(**{**valid_config_defaults, "max_listings": 10000})
    assert config.max_listings == 10000


@pytest.mark.parametrize("invalid_listings", [
    0,                # Too Low
    -99,              # Negative
    10001              # Too High
])
def test_invalid_max_listings(valid_config_defaults, invalid_listings):
    with pytest.raises(ValidationError):
        sc.SearchConfigParams(**{**valid_config_defaults, "max_listings": invalid_listings})
