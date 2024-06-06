import pytest
import json
import logging
from collections import namedtuple
from mixpanel_loader import MixpanelLoader

with open("tests/test_data.json") as file:
    test_data = json.load(file)

TestCase = namedtuple("TestCase", ["event", "expected"])


@pytest.fixture(autouse=True)
def env_setup(monkeypatch):
    monkeypatch.setenv("KAFKA_BROKER", "localhost")
    monkeypatch.setenv("ENV", "local-test")
    monkeypatch.setenv("DD_AGENT_HOST", "datadog-host")


# capture logs from INFO level
@pytest.fixture(autouse=True)
def configure_caplog_level(caplog):
    caplog.set_level(logging.INFO)


@pytest.fixture
def mock_kafka_message():
    KafkaMessage = namedtuple('KafkaMessage', ['topic', 'partition', 'offset', 'key', 'value', 'headers'])
    message = KafkaMessage(**test_data['kafka_message_pass'])
    return message


@pytest.fixture
def mock_bad_kafka_message():
    KafkaMessage = namedtuple('KafkaMessage', ['topic', 'partition', 'offset', 'key', 'value', 'headers'])
    message = KafkaMessage(**test_data['kafka_message_fail'])
    return message


@pytest.fixture(
    params=[
        TestCase(test_data["kafka_client_event"], test_data["kafka_client_event_expected_result"]),
        TestCase(test_data["kafka_client_missing_values"],
                 test_data["kafka_client_missing_values_event_expected_result"]),
    ]
)
def transform_to_mp_event_test_case(request):
    return request.param


@pytest.fixture(
    params=[
        TestCase(test_data["processed_event"], test_data["expected_processed_event"]),
        TestCase(test_data["processed_event_missing_values"], test_data["expected_processed_event_missing_values"]),
    ]
)
def transform_to_processed_event_test_case(request):
    return request.param


@pytest.fixture
def mixpanel_loader_mock():
    return MixpanelLoader(mixpanel_project_id="12345",
                          mixpanel_user="mixpanel_user",
                          mixpanel_password="mixpanel_password",
                          project_token="project_token")


@pytest.fixture(
    params=[
        TestCase(test_data["set_property_event"], test_data["expected_set_property_event_set_once"]),
    ]
)
def set_property_event_test_case(request):
    return request.param


@pytest.fixture(
    params=[
        TestCase(test_data["set_property_event"], test_data["expected_identify_event"]),
    ]
)
def identify_event_test_case(request):
    return request.param


@pytest.fixture
def event_batch_mock():
    transformed_event = json.dumps(test_data["kafka_client_event_expected_result"])
    return [transformed_event]
