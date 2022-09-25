import json
import os

from pyflink.common import *
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.table import StreamTableEnvironment

from functions import MapStats, EventKeySelector
from parse_args import parse_args
from schema import json_schema

if __name__ == '__main__':
    # get cli params
    args = parse_args()

    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)
    env.set_parallelism(1)
    flink_kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                   'jars/flink-sql-connector-kafka-1.15.2.jar')
    env.add_jars("file:///{}".format(flink_kafka_jar))

    deserialization_schema = JsonRowDeserializationSchema.builder().json_schema(json.dumps(json_schema)).build()
    kafka_consumer = FlinkKafkaConsumer(
        topics=args.topic,
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': args.bootstrap_servers, 'security.protocol': 'PLAINTEXT',
                    'group.id': 'flink_consumer'})

    ds = env.add_source(kafka_consumer)
    ds.key_by(EventKeySelector(), key_type=Types.STRING()).map(MapStats(), output_type=Types.STRING()).print()
    env.execute()
