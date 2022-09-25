from pyflink.datastream import MapFunction, RuntimeContext, KeySelector
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor, StateTtlConfig, MapState
from pyflink.common import *


class EventKeySelector(KeySelector):
    def get_key(self, value):
        return value['home_team']


class MapStats(MapFunction):
    def __init__(self):
        self.home_name = None
        self.away_name = None
        self.match_time = None
        self.match_stats = None
        self.ttl_config = StateTtlConfig.new_builder(Time.hours(2)).build()
        self.default_stats = {'GOAL': 0, 'SAVE': 0, 'SHOT': 0, 'SHOT_ON_TARGET': 0, 'SHOT_OFF_TARGET': 0, 'PASS': 0,
                              'PASS_COMPLETE': 0, 'CORNER': 0, 'INTERCEPTED': 0, 'FOUL_COMMIT': 0, 'OFFSIDE': 0,
                              'NO_BOOKING': 0, 'YELLOW': 0, 'RED': 0, 'POSSESSION': 0}

    def ball_possession(self, a: int, b: int) -> (float, float):
        total = a + b
        if total == 0:
            return 0, 0
        a_percent = round((a / total) * 100, 2)
        b_percent = round((b / total) * 100, 2)
        return a_percent, b_percent

    def open(self, runtime_context: RuntimeContext):
        home_team = ValueStateDescriptor('home_team', Types.STRING())
        away_team = ValueStateDescriptor('away_team', Types.STRING())
        match = MapStateDescriptor('match_stats', Types.STRING(), Types.MAP(Types.STRING(), Types.FLOAT()))

        home_team.enable_time_to_live(self.ttl_config)
        away_team.enable_time_to_live(self.ttl_config)
        match.enable_time_to_live(self.ttl_config)

        self.home_name = runtime_context.get_state(home_team)
        self.away_name = runtime_context.get_state(away_team)
        self.match_stats = runtime_context.get_map_state(match)

    def map(self, data):
        home = self.home_name.value()
        away = self.away_name.value()
        current_team = data['event_associated_team']
        game_time = data['game_time']

        if home is None:
            home = data['home_team']
            self.home_name.update(home)
        if away is None:
            away = data['away_team']
            self.away_name.update(away)

        if self.match_stats.get(home) is None:
            self.match_stats.put(home, self.default_stats)
        if self.match_stats.get(away) is None:
            self.match_stats.put(away, self.default_stats)

        current_home = self.match_stats.get(home)
        current_away = self.match_stats.get(away)

        if current_team == home:
            current_home[data['event_name']] = current_home.get(data['event_name'], 0) + 1
        else:
            current_away[data['event_name']] = current_away.get(data['event_name'], 0) + 1

        # BALL POSSESSION
        b_pos = self.ball_possession(data['home_possession_time'], data['away_possession_time'])
        current_home['POSSESSION'] = round(b_pos[0], 2)
        current_away['POSSESSION'] = round(b_pos[1], 2)

        # PASS PERCENTAGE
        current_home['PASS %'] = round(
            0 if current_home['PASS'] == 0 else ((current_home['PASS_COMPLETE'] / current_home['PASS']) * 100), 2)
        current_away['PASS %'] = round(
            0 if current_away['PASS'] == 0 else ((current_away['PASS_COMPLETE'] / current_away['PASS']) * 100), 2)

        self.match_stats.put(home, current_home)
        self.match_stats.put(away, current_away)

        home_stats = []
        away_stats = []
        for k, v in self.match_stats.get(home).items():
            home_stats.append(f"{k} -> {v}")

        for k, v in self.match_stats.get(away).items():
            away_stats.append(f"{k} -> {v}")

        return f"Game Time -> {game_time}" + "\n" + home.upper() + ' :: ' + ' '.join(
            str(e) for e in home_stats) + "\n" + away.upper() + ' :: ' + ' '.join(str(e) for e in away_stats)
