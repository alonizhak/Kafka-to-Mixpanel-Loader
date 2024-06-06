import logging
import json
import os
import traceback
import time
from datetime import datetime
from enum import Enum
from datadog import initialize, statsd
from kafka import KafkaConsumer, KafkaProducer
from mixpanel_loader import MixpanelLoader
from config import config_dict

logging.basicConfig(level=logging.INFO,
                    format='{"Timestamp":"%(asctime)s",  "level": "%(levelname)s", "message": "%(message)s"}',
                    handlers=[logging.StreamHandler()])

logger = logging.getLogger('kafka')
logger.setLevel(logging.WARN)


class MixpanelUploader:
    """Consume mixapnel-related records from Kafka topics,
    upload records to mixpanel via mixpanel import api.
    """

    def __init__(self):
        self.kafka_server = os.environ['KAFKA_BROKER']
        self.env = os.environ['ENV']
        self.dead_letter_topic = 'dead-letter'
        self.config = self.load_config()
        self.consumer = self.initialize_kafka_consumer()
        self.mixpanel_loader = self.initialize_mixpanel_loader()
        self.producer = self.initialize_kafka_producer()

    def load_config(self):
        if self.env == 'staging':
            config_dict['max_batch_size'] = 250
        elif self.env == 'local-test':
            config_dict['max_batch_size'] = 0
        return config_dict

    def initialize_kafka_consumer(self):
        logging.info("Initializing Kafka Consumer")
        return KafkaConsumer(
            bootstrap_servers=self.kafka_server.split(","),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='mixpanel-uploader-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=1000 * self.config["max_batch_time"],
        )

    def initialize_mixpanel_loader(self):
        logging.info("Initializing Mixpanel Loader")
        return MixpanelLoader(
            mixpanel_project_id=os.environ["MIXPANEL_PROJECT_ID"],
            mixpanel_user=os.environ["MIXPANEL_USER"],
            mixpanel_password=os.environ["MIXPANEL_PASSWORD"],
            project_token=os.environ["PROJECT_TOKEN"],
        )

    def initialize_kafka_producer(self):
        logging.info("Initializing Kafka Producer")
        return KafkaProducer(
            bootstrap_servers=self.kafka_server,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )

    def send_to_dead_letter(self, event_data, original_topic, error_message=None, error_details=None):
        try:
            timestamp = datetime.utcnow().isoformat()
            message = {
                "service": "mixpanel-uploader",
                "originalTopic": original_topic,
                "timestamp": timestamp,
                "errorMessage": error_message,
                "errorDetails": error_details,
                "payload": event_data,
            }
            json_message = message
            logging.info("sending message to dead letter... ")
            self.producer.send(self.dead_letter_topic, value=json_message)
            logging.info("message sent dead letter... ")
            self.producer.flush()
        except Exception:
            logging.error("Could not send bad message to dead letter topic")
            logging.error(traceback.format_exc())

    @staticmethod
    def initialize_datadog(config):
        initialize(**config)
        logging.info("Datadog.api and Datadog.statsd modules initialized")

    @staticmethod
    def add_event_to_batch(event, batch, byte_count):
        byte_count += len(event)
        batch.append(event)
        return batch, byte_count

    def consume_messages(self, events, batch_bytes):
        start_time = time.time()
        timeout_seconds = 60

        if len(events) > 0:
            logging.info("Reached timeout with queued events")
            self.mixpanel_loader.publish_current_batch(events)
            events.clear()
            batch_bytes = 0

        for kafka_event in self.consumer:
            try:
                logging.info(f'read new msg!')
                group_id = next(
                    (value.decode('utf-8') for key, value in kafka_event.headers if key == 'x-group'),
                    'mixpanel-uploader-group')
                if group_id != 'mixpanel-uploader-group':
                    continue

                if kafka_event.topic != self.config["mixpanel_data_topic"]:
                    if kafka_event.value.get("eventId"):
                        event_data = kafka_event.value["payload"]
                    else:
                        event_data = kafka_event.value

                    # filter out known bots
                    if event_data["device"].get("user_agent") in self.config["bots_ua_filter"]:
                        logging.info("Detected known bot. Applying filter.")
                        statsd.increment('mixpanel_uploader_metric.bots_counter',
                                         tags=[f"environment:{os.environ['ENV']}"])
                        continue

                    if event_data["event_name"] in self.config["identify_events"]:
                        mp_event_identify = self.mixpanel_loader.transform_to_mp_identify_event(event_data)
                        events, batch_bytes = self.add_event_to_batch(mp_event_identify, events, batch_bytes)

                    mp_event = self.mixpanel_loader.transform_to_mp_event(event_data)
                else:
                    logging.info(f"Event picked up from processed kafka message output")
                    mp_event = self.mixpanel_loader.transform_processed_to_mp_event(kafka_event.value)
                events, batch_bytes = self.add_event_to_batch(mp_event, events, batch_bytes)
                logging.info(f'current batch size: {len(events)}')

                elapsed_time = time.time() - start_time
                if elapsed_time >= timeout_seconds:
                    logging.info("Reached batch window timeout..")
                    break

                if len(events) > self.config["max_batch_size"] or batch_bytes >= self.config["bytes_per_batch"]:
                    logging.info("Reached max batch size")
                    self.mixpanel_loader.publish_current_batch(events)
                    events.clear()
                    batch_bytes = 0

            except Exception as e:
                error_message = f"mixpanel-uploader service encountered " \
                                f"the following issue processing this message: {e}"
                logging.error(error_message)
                self.send_to_dead_letter(kafka_event.value, kafka_event.topic, error_message=error_message,
                                         error_details=traceback.format_exc())
                logging.error(traceback.format_exc())

    def run(self):
        logging.info("Initializing Consume")
        self.consumer.subscribe(topics=self.config["source_kafka_topic"])
        self.initialize_datadog({'statsd_host': os.environ['DD_AGENT_HOST'], 'statsd_port': 8125})
        events = []
        batch_bytes = 0
        while True:
            try:
                self.consume_messages(events, batch_bytes)
            except Exception:
                logging.error(f"Error detected while running consumer: {traceback.format_exc()}")


if __name__ == "__main__":
    logging.info(f"{os.environ['ENV']} environment is booted")
    logging.info("Mixpanel-Uploader Service Started")
    mixpanel_uploader = MixpanelUploader()
    mixpanel_uploader.run()
