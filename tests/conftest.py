import pytest
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# Invoking this fixture: 'function_scoped_container_getter' starts all services
@pytest.fixture(scope="function")
def wait_for_api(function_scoped_container_getter):
    """Wait for the api from my_api_service to become responsive"""
    request_session = requests.Session()
    retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    request_session.mount("http://", HTTPAdapter(max_retries=retries))

    service = function_scoped_container_getter.get("my_api_service").network_info[0]
    api_url = f"http://{service.hostname}:{service.host_port}/"
    assert request_session.get(api_url)
    return api_url


def test_read_and_write(wait_for_api):
    """The Api is now verified good to go and tests can interact with it"""
    api_url = wait_for_api
    assert api_url
