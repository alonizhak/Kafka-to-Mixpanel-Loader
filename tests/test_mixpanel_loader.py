import json
from mixpanel_loader import MixpanelLoader
from main import MixpanelUploader


def test_transform_to_mp_event(transform_to_mp_event_test_case):
    """
      Test that the transform_to_mp_event() method in MixpanelUploader.

      Args:
          transform_to_mp_event_test_case: Pytest mocker fixture for mocking objects and functions.

      test accepts a client message and runs the transform_to_mp_event transform function.
      the asserts verifies the transformation is as expected.
      fail could mean that a change was made to either the transform function or before, that changed the
      expected output to something new.
      """
    result = MixpanelLoader.transform_to_mp_event(transform_to_mp_event_test_case.event)
    result = json.loads(result)
    assert result == transform_to_mp_event_test_case.expected


def test_transform_processed_mp_event(transform_to_processed_event_test_case):
    """
       Test that the transform_processed_mp_event() method in MixpanelUploader.
       Asserts the processed transform output is as expected.
       """
    result = MixpanelLoader.transform_processed_to_mp_event(transform_to_processed_event_test_case.event)
    result = json.loads(result)
    assert result == transform_to_processed_event_test_case.expected


def test_transform_to_mp_identify_event_event(identify_event_test_case):
    """
       Test that the transform_to_mp_identify_event method.
       Asserts the identify transform output is as expected.
       """
    result = MixpanelLoader.transform_to_mp_identify_event(identify_event_test_case.event)
    result = json.loads(result)
    assert result == identify_event_test_case.expected


def test_publish_current_batch_bad_request(mocker, mixpanel_loader_mock, event_batch_mock, caplog):
    """
       The test runs the publish_current_batch() method. The test uses dummy credentials for mixpanel,
       so the expected outcome should be the inability to publish batch to mixpanel and enter the dead-letter flow.
       Asserts is made on the error logs.
       """
    mocker.patch("mixpanel_loader.MixpanelLoader._send_to_dead_letter_queue")
    mixpanel_loader_mock.publish_current_batch(event_batch_mock)
    assert "Failed to send batch ID" in caplog.text


def test_consume_messages(mocker, mock_kafka_message, caplog):
    """
       Tests the full consume_messages() flow.
       Assert is made when we reach end oof flow and trigger the "Reached max batch size" message,
       'max_batch_size' = 0 in test-mode, so should trigger after first message.
       """
    mock_consumer = mocker.Mock()
    mocker.patch("main.MixpanelUploader.initialize_kafka_consumer", return_value=mock_consumer)
    mocker.patch("main.MixpanelUploader.initialize_kafka_producer", return_value=mocker.Mock())
    mocker.patch("main.statsd")
    mocker.patch("main.MixpanelUploader.initialize_mixpanel_loader")
    mock_consumer.__iter__ = mocker.Mock(return_value=iter([mock_kafka_message]))
    m = MixpanelUploader()
    m.consume_messages(events=[], batch_bytes=0)
    assert "Reached max batch size" in caplog.text


def test_consume_messages_dead_letter(mocker, mock_bad_kafka_message, caplog):
    """
       Tests the full consume_messages() flow with a bad kafka message.
       Assert is made on the error log, asserting message is handled by the dead-letter queue.
       """
    mock_consumer = mocker.Mock()
    mocker.patch("main.MixpanelUploader.initialize_kafka_consumer", return_value=mock_consumer)
    mocker.patch("main.MixpanelUploader.initialize_kafka_producer", return_value=mocker.Mock())
    mocker.patch("main.statsd")
    mocker.patch("main.MixpanelUploader.initialize_mixpanel_loader")
    mock_consumer.__iter__ = mocker.Mock(return_value=iter([mock_bad_kafka_message]))
    m = MixpanelUploader()
    m.consume_messages(events=[], batch_bytes=0)
    assert "message sent dead letter" in caplog.text
