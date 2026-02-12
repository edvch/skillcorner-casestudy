import numpy as np
import pandas as pd
import json
import pyarrow
from casestudy.pipeline.aggregation.DynamicEventsAggregator import DynamicEventAggregator
from casestudy.pipeline.ingestion.fetch_data import get_match_events_data

def aggregate_match_events(match_id):
    events_df = get_match_events_data(match_id)

    events_df['n_player_targeted_teammates_within_5m_start'] = np.zeros(len(events_df))
    events_df['n_player_targeted_opponents_within_5m_start'] = np.zeros(len(events_df))

    events_aggregator = DynamicEventAggregator(df=events_df)

    # Off-ball runs
    off_ball_runs = events_aggregator.generate_aggregates(
        group_by=["player_id", "player_name"], aggregate_type="off_ball_runs"
    )

    # Line-breaking passes
    line_breaking_passes = events_aggregator.generate_aggregates(
        group_by=["player_in_possession_id", "player_in_possession_name"],
        aggregate_type="line_breaking_passes",
    )

    # Defensive engagements
    defensive_engagements = events_aggregator.generate_aggregates(
        group_by=["player_id", "player_name"], aggregate_type="on_ball_engagements"
    )

    # Pressing
    pressing = events_aggregator.generate_aggregates(
        group_by=["player_id", "player_name"], aggregate_type="pressing_engagements"
    )

    contexts = {
        "custom": {
            "all_possessions": (
                (events_df["event_type"] == "player_possession")
                & (events_df["team_in_possession_phase_type"] == "finish")
                & (events_df["separation_start"] >= 5)
            )
        }
    }
    
    metric = {
    "custom": {
        "count": lambda x: len(x),
        "avg_duration": lambda x: x["duration"].mean(),
        "avg_distance_covered": lambda x: x["distance_covered"].mean(),
        }
    }
    custom_events_aggregator = DynamicEventAggregator(
        df=events_df, custom_context_groups=contexts, custom_metric_groups=metric)

    # Generate custom aggregates
    custom_aggregates = custom_events_aggregator.generate_aggregates(
        group_by=["player_id", "player_name"], aggregate_type="custom")
    
    return custom_aggregates