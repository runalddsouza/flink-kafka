from stopwatch import Stopwatch
import random


class Team:
    def __init__(self, home, away):
        self.home = home
        self.away = away
        self.possession = random.choice([home, away])
        self.home_timer = Stopwatch()
        self.away_timer = Stopwatch()

    def stop_time(self):
        self.home_timer.stop()
        self.away_timer.stop()

    def sync_time(self):
        if self.possession == self.home:
            self.home_timer.start()
            self.away_timer.stop()
        else:
            self.away_timer.start()
            self.home_timer.stop()

    def switch_possession(self):
        if self.possession == self.home:
            self.possession = self.away
        else:
            self.possession = self.home
        self.sync_time()
