import json
import random
import uuid

from confluent_kafka import Producer

import logger
from parse_args import parse_args

from stopwatch import Stopwatch
import time
from time import gmtime
from time import strftime

import event_type
from event_type import *
from team import Team


class FootballEventProducer:
    def __init__(self, producer, topic_name, log, match_id: str, home_team: str, away_team: str):
        self.__attempt = 1
        self.__log = log
        self.producer = producer
        self.topic_name = topic_name
        self.home_team = home_team
        self.away_team = away_team
        self.duration = 60 * 90
        self.match_id = match_id

    def publish(self, key, value):
        try:
            self.producer.produce(topic=self.topic_name, key=key, value=value, on_delivery=self.acked)
            self.producer.poll(0)
            self.producer.flush()
            self.__log.info(value)
        except Exception as e:
            self.__log.error("Unknown failure ; Exception: " + str(type(e).__name__) + "  ; message: " + str(e))

    def acked(self, err, msg):
        """Delivery report handler called on successful or failed delivery of message """
        if err is not None:
            self.__log.error("Failed to deliver message: {}".format(err))
        else:
            self.__log.info("Produced record to topic {} partition [{}] @ offset {}"
                            .format(msg.topic(), msg.partition(), msg.offset()))

    def get_duration(self, time_in_seconds: int) -> str:
        return strftime("%M:%S", gmtime(time_in_seconds))

    def generate_event(self, base_event) -> Enum:
        return random.choices(get_events(base_event), weights=get_events(base_event, weight=True), k=1)[0]

    def start(self):
        teams = Team(self.home_team, self.away_team)
        game_timer = Stopwatch()
        e = event_type.BaseEvent
        game_timer.start()
        teams.sync_time()

        while game_timer.duration < self.duration:
            time.sleep(random.randint(3, 6))
            new_event = self.generate_event(e)
            key = str(uuid.uuid4())
            message = json.dumps({
                "id": key,
                "event_time": int(time.time() * 1000),
                "match_id": self.match_id,
                "home_team": self.home_team,
                "away_team": self.away_team,
                "event_name": new_event.name,
                "event_associated_team": teams.possession,
                "game_time": self.get_duration(int(game_timer.duration)),
                "home_possession_time": int(teams.home_timer.duration),
                "away_possession_time": int(teams.away_timer.duration)
            }, ensure_ascii=False)
            self.publish(key, message)
            if get_delay_mapping(new_event):
                teams.stop_time()
                time.sleep(random.randint(1, 5))
                teams.sync_time()
            mapping = get_event_mapping(new_event)
            if mapping == Possession.TURN_OVER:
                teams.switch_possession()
                e = event_type.BaseEvent
            else:
                e = mapping

        game_timer.stop()
        teams.stop_time()


if __name__ == '__main__':
    # get cli params
    args = parse_args()
    logger = logger.AppLogger(args.log_file).get_logger()

    # kafka
    producer_conf = {'bootstrap.servers': args.bootstrap_servers, 'security.protocol': 'PLAINTEXT'}
    topic = args.topic
    kafka_producer = Producer(producer_conf)
    app = FootballEventProducer(kafka_producer, topic, logger, str(uuid.uuid4()), args.home, args.away)
    app.start()
