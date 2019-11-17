import json
import pytest
import datetime
from munch import DefaultMunch

from strela.gateway.emx import streaming, execution
from strela.gateway.emx.shared_storage import shared_storage

from strela.positions_manager import positions_manager
from strela.monitoring_adapter import monitoring_adapter


@pytest.fixture
def cfg_fixture():
    b = DefaultMunch()
    b.url = "test_url"
    b.api_key = "test_key"
    b.api_secret = "test_secret"

    b.exchanges = DefaultMunch()
    b.exchanges.names = ['emx']

    b.exchange_name = "emx"
    return b

@pytest.mark.asyncio
async def test_rest_positions(cfg_fixture):
    pass
