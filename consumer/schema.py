json_schema = {
    "definitions": {
        "record:FootballMatchEvent": {
            "type": "object",
            "required": ["id", "event_time", "match_id", "home_team", "away_team", "event_name",
                         "event_associated_team", "game_time", "home_possession_time", "away_possession_time"],
            "properties": {
                "id": {
                    "type": "string"
                },
                "event_time": {
                    "type": "integer",
                    "minimum": -9223372036854775808,
                    "maximum": 9223372036854775807
                },
                "match_id": {
                    "type": "string"
                },
                "home_team": {
                    "type": "string"
                },
                "away_team": {
                    "type": "string"
                },
                "event_name": {
                    "type": "string"
                },
                "event_associated_team": {
                    "type": "string"
                },
                "game_time": {
                    "type": "string"
                },
                "home_possession_time": {
                    "type": "integer",
                    "minimum": -2147483648,
                    "maximum": 2147483647
                },
                "away_possession_time": {
                    "type": "integer",
                    "minimum": -2147483648,
                    "maximum": 2147483647
                }
            }
        }
    },
    "$ref": "#/definitions/record:FootballMatchEvent"
}
