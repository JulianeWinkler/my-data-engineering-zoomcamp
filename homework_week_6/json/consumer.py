from typing import Dict, List
from json import loads
from kafka import KafkaConsumer

from ride import Ride_fhv, Ride_green
import settings as st


class JsonConsumer:
    def __init__(self, props: Dict):
        self.consumer = KafkaConsumer(**props)

    def consume_from_kafka(self, topics: List[str]):
        self.consumer.subscribe(topics)
        print('Consuming from Kafka started')
        print('Available topics to consume: ', self.consumer.subscription())
        while True:
            try:
                # SIGINT can't be handled when polling, limit timeout to 1 second.
                message = self.consumer.poll(1.0)
                if message is None or message == {}:
                    continue
                for message_key, message_value in message.items():
                    for msg_val in message_value:
                        print(msg_val.key, msg_val.value)
            except KeyboardInterrupt:
                break

        self.consumer.close()


if __name__ == '__main__':
    config_fhv = {
        'bootstrap_servers': st.BOOTSTRAP_SERVERS,
        'auto_offset_reset': 'earliest',
        'enable_auto_commit': True,
        'key_deserializer': lambda key: int(key.decode('utf-8')),
        'value_deserializer': lambda x: loads(x.decode('utf-8'), object_hook=lambda d: Ride_fhv.from_dict(d)),
        'group_id': 'consumer.group.id.json-example.1',
    }

    config_green = {
        'bootstrap_servers': st.BOOTSTRAP_SERVERS,
        'auto_offset_reset': 'earliest',
        'enable_auto_commit': True,
        'key_deserializer': lambda key: int(key.decode('utf-8')),
        'value_deserializer': lambda x: loads(x.decode('utf-8'), object_hook=lambda d: Ride_green.from_dict(d)),
        'group_id': 'consumer.group.id.json-example.1',
    }

    json_consumer_fhv = JsonConsumer(props=config_fhv)
    json_consumer_green = JsonConsumer(props=config_green)
    json_consumer_fhv.consume_from_kafka(topics=[st.KAFKA_TOPIC_FHV])
    json_consumer_green.consume_from_kafka(topics=[st.KAFKA_TOPIC_GREEN])