import json
import uuid

from kafka import KafkaProducer, KafkaConsumer, TopicPartition

# template adapted a little by setting some instance variables with parameters in the constructor

class Kafka:
    def __init__(self, bootstrap_servers, topic):
        self.kafka_bootstrap_server = bootstrap_servers
        self.kafka_topic = topic
        self.topic_partition = TopicPartition(self.kafka_topic, 0)

    producer: KafkaProducer
    consumer: KafkaConsumer

    def close(self):
        self.producer.close()
        self.consumer.close()


class KafkaWriter(Kafka):
    def __init__(self, bootstrap_servers, topic):
        super(KafkaWriter, self).__init__(bootstrap_servers, topic)
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_server,
                key_serializer=str.encode,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        except ValueError as ve:
            # might hapen when the bootstrap server is unreachable
            print(f'  !!! error while creating kafka producer: {ve}')

    def store(self, message_key: str, data: json) -> None:
        if self.producer is not None:
            self.producer.send(self.kafka_topic, key=message_key, value=data)
            self.producer.flush()


class KafkaReader(Kafka):
    def __init__(self, bootstrap_servers, topic, group_id, client_id, auto_offset_reset='earliest', update_offsets=True):
        super(KafkaReader, self).__init__(bootstrap_servers, topic)
        self.update_offsets=update_offsets
        self.group_id=group_id
        self.client_id=client_id
        try:
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.kafka_bootstrap_server,
                group_id=self.group_id,
                client_id=self.client_id,
                auto_offset_reset=auto_offset_reset,
                enable_auto_commit=True,

                key_deserializer=lambda k: k.decode('utf-8'),
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
        except ValueError as ve:
            # might hapen when the bootstrap server is unreachable
            print(f'  !!! error while creating kafka consumer: {ve}')
        if self.consumer is not None:
            self.consumer.assign([self.topic_partition])

    def retrieve(self) -> {}:
        predictions = []
        if self.consumer is not None:
            while True:  # poll until the size of returned messages is zero, then break the loop
                messages = self.consumer.poll(timeout_ms=3000, update_offsets=self.update_offsets)
                if len(messages) == 0:
                    # no more messages in topic
                    break

                for key in messages.keys():
                    records = messages[key]
                    for record in records:
                        predictions.append(record.value)
        return predictions
