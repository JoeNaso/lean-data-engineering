
from lean_data_eng import resources


def test_local_and_prod_resources_match():
    local = resources.resources.get('local')
    prod = resources.resources.get('production')
    diff = set(local.keys()) - set(prod.keys())
    assert diff == set()