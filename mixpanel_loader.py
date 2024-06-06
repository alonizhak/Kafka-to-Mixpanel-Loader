import json
import logging
import gzip
import requests
import os
import uuid
import time
from enum import Enum
from typing import List
from datetime import datetime
from datadog import statsd
from kafka import KafkaProducer

# Test
EVENT_NAME_MAPPING = {
    "user-purchased": "user_purchased",
    "user-feedback": "user_feedback",
    "user-created": "user_created",
    "user-updated": "user_updated",
    "user-deleted": "user_deleted"
}

MIXPANEL_PROFILE_SET_URL = "https://api.mixpanel.com/engage?verbose=1#profile"
MAX_RETRY_ATTEMPTS = 3
DEAD_LETTER_TOPIC = "dead-letter"

class MixpanelLoader:
    def __init__(self, mixpanel_project_id, mixpanel_user, mixpanel_password, project_token):
        """
        Initialize MixpanelLoader with Mixpanel credentials and project token.

        :param mixpanel_project_id: ID of the Mixpanel project
        :param mixpanel_user: Mixpanel user for authentication
        :param mixpanel_password: Password for the Mixpanel user
        :param project_token: Project token for Mixpanel
        """
        self.mixpanel_project_id = mixpanel_project_id
        self.mixpanel_user = mixpanel_user
        self.mixpanel_password = mixpanel_password
        self.project_token = project_token

    @staticmethod
    def publish_user_property(event, set_type: str):
        """
        Publish a user property to Mixpanel.

        :param event: The event containing user property data
        :param set_type: The type of set operation to perform (e.g., 'set', 'set_once')
        """
        logging.info("publishing event with user property data...")

        url = f"{MIXPANEL_PROFILE_SET_URL}-{set_type}"
        headers = {"accept": "text/plain", "content-type": "application/json"}
        response = requests.post(url, json=[json.loads(event)], headers=headers)

        if response.status_code not in (200, 201, 202, 203, 204):
            logging.error(f"{json.dumps(response.text)}")
            # Need to add dead letter queue and error handler to datadog
            pass
        else:
            logging.info(f"{set_type} successfully made")
            logging.info(f"user property data successfully updated")

    def publish_current_batch(self, events):
        """
        Publish a batch of events to Mixpanel.

        :param events: List of events to be sent to Mixpanel
        """
        batch_size = len(events)
        batch_id = uuid.uuid4()
        logging.info(f"sending batch ID {batch_id} to mixpanel...")

        url = "https://api.mixpanel.com/import"
        headers = {
            "Content-Type": "application/x-ndjson",
            "Content-Encoding": "gzip",
            "User-Agent": "mixpanel-s3",
        }
        params = {"strict": "1", "project_id": self.mixpanel_project_id}
        payload = gzip.compress("\n".join(events).encode("utf-8"))

        retry_attempts = 0
        backoff_time = 1  # Initial backoff time in seconds
        response = None

        while retry_attempts < MAX_RETRY_ATTEMPTS:
            response = requests.post(
                url,
                auth=(self.mixpanel_user, self.mixpanel_password),
                data=payload,
                headers=headers,
                params=params,
            )

            if response.status_code in (200, 201, 202, 203, 204):
                logging.info(
                    f"batch ID {batch_id} with {batch_size} events successfully sent to mixpanel"
                )
                statsd.increment(
                    "mixpanel_uploader_metric.batch_size",
                    value=batch_size,
                    tags=[f"environment:{os.environ['ENV']}"],
                )
                return  # Request succeeded, exit the function

            retry_attempts += 1
            logging.warning(
                f"Failed to send batch ID {batch_id} to mixpanel. Retrying in {backoff_time} seconds..."
            )

            time.sleep(backoff_time)
            backoff_time *= 2  # Exponential backoff, double the backoff time

        # Request failed after max_retry_attempts
        logging.error(
            f"Failed to send batch ID {batch_id} to mixpanel after {MAX_RETRY_ATTEMPTS} attempts."
        )
        logging.error(f"{json.dumps(response.text)}")

        # Add dead letter queue and error handler to datadog
        self._send_to_dead_letter_queue(events, batch_id, response=response.text)

    @staticmethod
    def transform_to_mp_event(kafka_event):
        """
        Transform a Kafka event to a Mixpanel event format.

        :param kafka_event: The original Kafka event
        :return: The transformed Mixpanel event as a JSON string
        """
        properties = kafka_event["properties"]
        event = {
            "event": kafka_event["event_name"],
            "properties": properties,
        }
        event["properties"].update(
            {
                "time": kafka_event["created_at"],
                "distinct_id": kafka_event.get("user_id", kafka_event.get("uid")),
                "$anon_distinct_id": kafka_event["uid"],
                "$insert_id": kafka_event["event_id"],
                "event_id": kafka_event["event_id"],
                "user_id": kafka_event.get("user_id"),
                "application": kafka_event["application"],
                "created_at_local": kafka_event["created_at_local"],
                "device_type": kafka_event["device"].get("device", {}).get("type"),
                "device_brand": kafka_event["device"].get("device", {}).get("brand"),
                "device_model": kafka_event["device"].get("device", {}).get("model"),
                "os_name": kafka_event["device"].get("os", {}).get("name"),
                "os_version": kafka_event["device"].get("os", {}).get("version"),
                "os_platform": kafka_event["device"].get("os", {}).get("platform"),
                "client_type": kafka_event["device"].get("client", {}).get("type"),
                "client_name": kafka_event["device"].get("client", {}).get("name"),
                "client_version": kafka_event["device"].get("client", {}).get("version"),
                "client_engine": kafka_event["device"].get("client", {}).get("engine"),
                "utm_source": kafka_event["utm"].get("source"),
                "utm_medium": kafka_event["utm"].get("medium"),
                "utm_campaign": kafka_event["utm"].get("campaign"),
                "utm_content": kafka_event["utm"].get("content"),
                "utm_term": kafka_event["utm"].get("term"),
                "$city": kafka_event["geo"].get("city"),
                "mp_country_code": kafka_event["geo"].get("countryCode"),
                "timezone": kafka_event["geo"].get("timeZone"),
                "continent": kafka_event["geo"].get("continent"),
            }
        )

        return json.dumps(event)

    @staticmethod
    def transform_to_mp_identify_event(kafka_event):
        """
        Transform a Kafka event to a Mixpanel identify event format.

        :param kafka_event: The original Kafka event
        :return: The transformed Mixpanel identify event as a JSON string
        """
        event = {
            "properties": {
                "distinct_id": kafka_event.get("user_id", kafka_event.get("uid")),
                "$anon_distinct_id": kafka_event["uid"],
            },
            "event": "$identify",
        }
        return json.dumps(event)

    @staticmethod
    def transform_processed_to_mp_event(kafka_event):
        """
        Transform a processed Kafka event to a Mixpanel event format.

        :param kafka_event: The processed Kafka event
        :return: The transformed Mixpanel event as a JSON string
        """
        properties = {}
        user_params = json.loads(kafka_event.get("user_params", "{}"))
        server_events_payload = json.loads(kafka_event.get("server_events_payload", "{}"))
        user_params_dict = {
            f"user_params.{key}": value for key, value in user_params.items()
        }
        kafka_event.pop("user_params", None)
        kafka_event.pop("server_events_payload", None)
        event_name = EVENT_NAME_MAPPING.get(kafka_event["event"], kafka_event["event"])
        event = {
            "event": event_name,
            "properties": properties,
        }
        event["properties"].update(kafka_event)
        event["properties"].update(user_params_dict)
        event["properties"].update(server_events_payload)
        event = MixpanelLoader._special_event_transform(event)
        return json.dumps(event)

    @staticmethod
    def _special_event_transform(event):
        """
        Perform special transformations on specific events.

        :param event: The event to be transformed
        :return: The transformed event
        """
        if event['event'] == "special_event":
            # Example transformation: add a new property to the event
            event['properties']['new_property'] = 'new_value'
        return event

    def _send_to_dead_letter_queue(self, events, batch_id, response):
        """
        Send events to the dead letter queue after failing to send to Mixpanel.

        :param events: List of events to be sent to the dead letter queue
        :param batch_id: The ID of the failed batch
        :param response: The response from the failed request
        """
        producer = KafkaProducer(
            bootstrap_servers=os.environ["KAFKA_BROKER"],
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        error_message = f"Failed to send batch ID {batch_id} to mixpanel after {MAX_RETRY_ATTEMPTS} attempts."
        for event in events:
            self.send_to_dead_letter(
                producer,
                event,
                original_topic="mixpanel-data",
                error_message=error_message,
                error_details=response,
            )
        producer.flush()
        logging.info(f"batch ID {batch_id} sent to dead letter queue")

    @staticmethod
    def send_to_dead_letter(producer, event_data, original_topic, error_message=None, error_details=None):
        """
        Send an individual event to the dead letter queue.

        :param producer: Kafka producer to send the event
        :param event_data: The event data to be sent
        :param original_topic: The original topic of the event
        :param error_message: Optional error message
        :param error_details: Optional error details
        """
        timestamp = datetime.utcnow().isoformat()
        message = {
            "service": "mixpanel-uploader",
            "originalTopic": original_topic,
            "timestamp": timestamp,
            "errorMessage": error_message,
            "errorDetails": error_details,
            "payload": event_data,
        }
        json_message = json.dumps(message)
        producer.send(DEAD_LETTER_TOPIC, value=json_message)
