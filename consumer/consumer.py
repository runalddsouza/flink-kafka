import json
import os

import avro
from pyflink.common import *
from pyflink.datastream import StreamExecutionEnvironment, KeySelector, MapFunction, RuntimeContext, ReduceFunction
from pyflink.datastream.connectors import FlinkKafkaConsumer, Source
from pyflink.datastream.state import ValueStateDescriptor, MapState, MapStateDescriptor, AggregatingState
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.udf import udf

from consumer import schema
from consumer.schema import avro_dict


def ball_possession(a: int, b: int) -> (float, float):
    total = a + b
    if total == 0:
        return 0, 0
    a_percent = round((a / total) * 100, 2)
    b_percent = round((b / total) * 100, 2)
    return a_percent, b_percent


# result_type = Types.ROW_NAMED(
#     ["TEAM", "GOAL", "SAVE", "SHOT", "SHOT_ON_TARGET", "SHOT_OFF_TARGET", "PASS", "PASS_COMPLETE",
#      "CORNER", "FOUL_COMMIT", "OFFSIDE", "NO_BOOKING", "YELLOW", "RED", "POS_A", "POS_B", "EVT_TIME",
#      "HOME", "AWAY"
#      ],
#     [Types.STRING(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(),
#      Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(),
#      Types.INT(), Types.INT(), Types.STRING(), Types.STRING(), Types.LONG(), Types.STRING(), Types.STRING()]
# )


class MapStats(MapFunction):

    def __init__(self):
        self.home_name = None
        self.away_name = None
        self.home_stats = None
        self.away_stats = None

    def open(self, runtime_context: RuntimeContext):
        home_team = ValueStateDescriptor('home_team', Types.STRING())
        away_team = ValueStateDescriptor('away_team', Types.STRING())
        home = MapStateDescriptor('home_stats', Types.STRING(), Types.FLOAT())
        away = MapStateDescriptor('away_stats', Types.STRING(), Types.FLOAT())
        self.home_name = runtime_context.get_state(home_team)
        self.away_name = runtime_context.get_state(away_team)
        self.home_stats = runtime_context.get_map_state(home)
        self.away_stats = runtime_context.get_map_state(away)

    def map(self, data):
        home = self.home_name
        away = self.away_name

        if home is None:
            home = data['home_team']
            self.home_name.update(home)
        if away is None:
            away = data['away_team']
            self.away_name.update(away)

        if data['event_associated_team'] == home:
            current_stats = self.home_stats
        else:
            current_stats = self.away_stats

        if current_stats.is_empty():
            current_stats.put('GOAL', 0)
            current_stats.put('SAVE', 0)
            current_stats.put('SHOT', 0)
            current_stats.put('SHOT_ON_TARGET', 0)
            current_stats.put('SHOT_OFF_TARGET', 0)
            current_stats.put('PASS', 0)
            current_stats.put('PASS_COMPLETE', 0)
            current_stats.put('CORNER', 0)
            current_stats.put('INTERCEPTED', 0)
            current_stats.put('FOUL_COMMIT', 0)
            current_stats.put('OFFSIDE', 0)
            current_stats.put('NO_BOOKING', 0)
            current_stats.put('YELLOW', 0)
            current_stats.put('RED', 0)
            current_stats.put('POSSESSION', 0)

        print(data['event_name'])
        current_stats.put(data['event_name'], current_stats.get(data['event_name']) + 1)
        b_pos = ball_possession(data['home_possession_time'], data['away_possession_time'])

        if data['event_associated_team'] == home:
            self.home_stats = current_stats
            self.home_stats.put('POSSESSION', b_pos[0])
            current_stats = self.home_stats
        else:
            self.away_stats = current_stats
            self.away_stats.put('POSSESSION', b_pos[1])
            current_stats = self.away_stats

        stats = []
        for k, v in current_stats.items():
            stats.append(f"{k} -> {v}")
        stats.sort()
        return data['event_associated_team'].upper() + ' :: ' + ' '.join(str(e) for e in stats)

        # return Row(
        #     str(data['event_associated_team']),
        #     1 if data['event_name'] == 'GOAL' else 0,
        #     1 if data['event_name'] == 'SAVE' else 0,
        #     1 if data['event_name'] == 'SHOT' else 0,
        #     1 if data['event_name'] == 'SHOT_ON_TARGET' else 0,
        #     1 if data['event_name'] == 'SHOT_OFF_TARGET' else 0,
        #     1 if data['event_name'] == 'PASS' else 0,
        #     1 if data['event_name'] == 'PASS_COMPLETE' else 0,
        #     1 if data['event_name'] == 'CORNER' else 0,
        #     1 if data['event_name'] == 'FOUL_COMMIT' else 0,
        #     1 if data['event_name'] == 'OFFSIDE' else 0,
        #     1 if data['event_name'] == 'NO_BOOKING' else 0,
        #     1 if data['event_name'] == 'YELLOW' else 0,
        #     1 if data['event_name'] == 'RED' else 0,
        #     str(f"{b_pos[0]} %"),
        #     str(f"{b_pos[1]} %"),
        #     int(data['event_time']),
        #     str(data['home_team']),
        #     str(data['away_team'])
        # )


class StatsAgg(ReduceFunction):
    def reduce(self, value1, value2) -> Row:
        return Row(str(value1[0]),
                   int(value1[1]) + int(value2[1]),
                   int(value1[2]) + int(value2[2]),
                   int(value1[3]) + int(value2[3]),
                   int(value1[4]) + int(value2[4]),
                   int(value1[5]) + int(value2[5]),
                   int(value1[6]) + int(value2[6]),
                   int(value1[7]) + int(value2[7]),
                   int(value1[8]) + int(value2[8]),
                   int(value1[9]) + int(value2[9]),
                   int(value1[10]) + int(value2[10]),
                   int(value1[11]) + int(value2[11]),
                   int(value1[12]) + int(value2[12]),
                   int(value1[13]) + int(value2[13]),
                   int(value1[14]) if value1[16] > value2[16] else value2[14],
                   value1[15] if value1[16] > value2[16] else value2[15],
                   value1[16] if value1[16] > value2[16] else value2[16],
                   str(value1[17]),
                   str(value1[18])
                   )


# class ParseStats(MapFunction):
#     def map(self, data) -> Row:
#         str = f"{data['HOME']} {data['GOAL']}- {data['AWAY']}\n"


# class MyReduceFunction(ReduceFunction):
#     def reduce(self, value1, value2):
#         if[]


env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)
env.set_parallelism(1)
flink_kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'jars/flink-sql-connector-kafka-1.15.2.jar')
# flink_avro_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'jars/flink-avro-1.15.2.jar')
# # avro_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'jars/avro-1.11.1.jar')
# avro_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'jars/avro-1.9.2.jar')
# #avro_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'jars/avro-1.10.0.jar')
# jackson_core_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'jars/jackson-core-2.13.4.jar')
# jackson_databind_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'jars/jackson-databind-2.13.4.jar')
# jackson_annotations_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
#                                        'jars/jackson-annotations-2.13.4.jar')
#
# print(avro_jar)
env.add_jars("file:///{}".format(flink_kafka_jar),
             # "file:///{}".format(flink_avro_jar),
             # "file:///{}".format(avro_jar),
             # "file:///{}".format(jackson_core_jar),
             # "file:///{}".format(jackson_databind_jar),
             # "file:///{}".format(jackson_annotations_jar)
             )

deserialization_schema = JsonRowDeserializationSchema.builder().json_schema(json.dumps(schema.json_schema)).build()
# avro.schema.parse(json.dumps(schema.avro_dict))
# deserialization_schema = AvroRowDeserializationSchema(avro_schema_string = str(avro.schema.parse(json.dumps(schema.avro_dict))))

# deserialization_schema = JsonRowDeserializationSchema.builder().type_info(
#                              type_info=Types.ROW_NAMED(
#                              ["id","match_id"], [Types.STRING(), Types.STRING()])).build()
# deserialization_schema = SimpleStringSchema()


kafka_consumer = FlinkKafkaConsumer(
    topics='FootballMatchEvent',
    deserialization_schema=deserialization_schema,
    properties={'bootstrap.servers': 'localhost:9092',
                'schema.registry.url': 'localhost:8081',
                'security.protocol': 'PLAINTEXT'
                })

ds = env.add_source(kafka_consumer)

# ds.map(MapStats(), output_type=result_type)

#     .key_by(lambda i: i[0]).print()

# print(ds.map(MapStats(), output_type=result_type).key_by(lambda i: i[0], key_type=Types.STRING()).reduce(
#     StatsAgg()).get_type())
# ds.map(MapStats(), output_type=result_type).key_by(lambda i: i[0], key_type=Types.STRING()).reduce(StatsAgg()).print()

ds.key_by(lambda i: i['event_associated_team']).map(MapStats(), output_type=Types.STRING()).print()

# print(ds.get_type())
# ds.key_by(lambda i: i['event_associated_team']).print()
# ds.print()

# t = t_env.from_data_stream(ds)
# t_env.create_temporary_view("football_match_events", t)
# res_table = t_env.sql_query("SELECT * FROM football_match_events")

#
# interpret the updating Table as a changelog DataStream
# res_ds = t_env.to_changelog_stream(res_table)
#
# # add a printing sink and execute in DataStream API
# res_ds.print()
env.execute()
# ds.execute_and_collect()
