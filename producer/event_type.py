from enum import Enum


def get_events(t, weight=False) -> list:
    if weight:
        return [e.value for e in t]
    return [e for e in t]


class BaseEvent(Enum):
    PASS = 1


class Possession(Enum):
    TURN_OVER = 1


class Pass(Enum):
    PASS_COMPLETE = 0.5
    INTERCEPTED = 0.3
    FOUL_COMMIT = 0.05
    OPPOSITION_FOUL = 0.05
    OFFSIDE = 0.1


class PassComplete(Enum):
    SHOT = 0.2
    PASS = 0.8


class Shot(Enum):
    SHOT_ON_TARGET = 0.3
    SHOT_OFF_TARGET = 0.7


class ShotOnTarget(Enum):
    GOAL = 0.1
    SAVE = 0.5
    CORNER = 0.4


class Corner(Enum):
    SHOT_ON_TARGET = 0.2
    SHOT_OFF_TARGET = 0.8


class Foul(Enum):
    NO_BOOKING = 0.8
    YELLOW = 0.15
    RED = 0.05


turn_over = [Pass.INTERCEPTED, Pass.FOUL_COMMIT, Pass.OFFSIDE,
             Shot.SHOT_OFF_TARGET,
             ShotOnTarget.SAVE, ShotOnTarget.GOAL,
             Corner.SHOT_OFF_TARGET
             ]

delay = [Pass.FOUL_COMMIT, Pass.OFFSIDE,
         Shot.SHOT_OFF_TARGET,
         ShotOnTarget.SAVE, ShotOnTarget.GOAL,
         Corner.SHOT_OFF_TARGET
         ]


def get_delay_mapping(e: Enum) -> bool:
    return e in delay


def get_event_mapping(e: Enum):
    if e in turn_over:
        return Possession.TURN_OVER
    elif e == BaseEvent.PASS:
        return Pass
    elif e == Pass.PASS_COMPLETE:
        return PassComplete
    elif e == Pass.OPPOSITION_FOUL:
        return Pass
    elif e == PassComplete.PASS:
        return Pass
    elif e == PassComplete.SHOT:
        return Shot
    elif e == Shot.SHOT_ON_TARGET:
        return ShotOnTarget
    elif e == ShotOnTarget.CORNER:
        return Corner
    elif e == Corner.SHOT_ON_TARGET:
        return ShotOnTarget
    else:
        raise ValueError('Not found: ' + str(e))
