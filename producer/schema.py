event_schema = {"id": str,
                "event_time": int,
                "match_id": str,
                "home_team": str,
                "away_team": str,
                "event_name": str,
                "event_associated_team": str,
                "game_time": str,
                "home_possession_time": int,
                "away_possession_time": int}

avro_dict = {
    "type": "record",
    "name": "FootballMatchEvent",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "event_time", "type": "long"},
        {"name": "match_id", "type": "string"},
        {"name": "home_team", "type": "string"},
        {"name": "away_team", "type": "string"},
        {"name": "event_name", "type": "string"},
        {"name": "event_associated_team", "type": "string"},
        {"name": "game_time", "type": "string"},
        {"name": "home_possession_time", "type": "int"},
        {"name": "away_possession_time", "type": "int"}
    ]
}
