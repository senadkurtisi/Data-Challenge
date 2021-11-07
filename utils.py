import json

import numpy as np
import pandas as pd


def load_dataset(dataset_path):
    """Loads the .jsonl dataset from desired path and decodes it.
    
    Arguments:
        dataset_path (str): Path where dataset is stored
    Returns:
        df (pandas.DataFrame): DataFrame with the loaded dataset.
    """
    if not dataset_path.endswith(".jsonl"):
        raise ValueError("Invalid dataset path! Only '.jsonl' extension is supported.")
    
    with open(dataset_path, "r") as json_file:
        event_list_raw = list(json_file)

    # Decode strings by following JSON format -> event=  Python dict
    event_list_decoded = [json.loads(event) for event in event_list_raw]
    
    df = pd.DataFrame(event_list_decoded)
    return df


def unflatten_event_data(df, target_columns, data_column="event_data"):
    """Unravels the df's data column to multiple columns.
    
    This columns is stored as a JSON object therefore this is possible.

    Arguments:
        df (pandas.DataFrame): DataFrame which contains data column
        target_columns (list of str): All columns that should come out of the data column
    Returns:
        df_copy (pandas.DataFrame): DataFrame with @data_column expanded to multiple other
    """
    df_copy = df.copy()
    # Not all events will be valid, i.e. some events may lack some of the required data
    # specified in the @target_columns argument. These events will be dropped later
    for col in target_columns:
        df_copy[col] = np.NaN

    # Decode the data column
    df_copy[target_columns] = df[data_column].apply(lambda x: pd.Series(x))
    return df_copy


def prepare_for_join(df, renaming_rules, to_drop, join_column):
    """Performs necessary processing of the dataferame before it can be joined."""
    # Main column based on which the join will be performed
    df = df.set_index(join_column)
    # Renaming columns in order to keep them after join, if there is an overlap
    df = df.rename(renaming_rules)
    # Drop unnecessary columns
    df = df.drop(columns=to_drop)
    return df


def join_match_data(df_match_start, df_match_end, join_column="match_id"):
    """Joins start match data with end match data.
    
    Main usage: Filtering the matches for which start_match-end_match pairs can't be found.
    
    Arguments:
        df_match_start (pandas.DataFrame): Contains match start data
        df_match_start (pandas.DataFrame): Contains match end data
        join_column (str): Column which is used for matching two DataFrames
    """
    cols_to_drop = ["event_type", "event_data"]
    # Prepare match dataframes for join
    start_renaming_rules = {
        "event_timestamp": "start_time",
        "event_id": "start_id"
    }
    end_renaming_rules = {
        "event_timestamp": "end_time",
        "event_id": "end_id"
    }
    df_match_start = prepare_for_join(df_match_start, start_renaming_rules, cols_to_drop, join_column)
    df_match_end = prepare_for_join(df_match_end, end_renaming_rules, cols_to_drop, join_column)
    # Automatically discard matches for which we can't find match_start-match_end pairs
    df_matches = df_match_start.join(df_match_end, on=join_column, how="inner").reset_index()
    return df_matches


def filter_goals(goal_data, df_matches):
    """Discards invalid goal events."""
    goal_time= goal_data["event_timestamp"]
    try:
        match_data = df_matches[df_matches["match_id"] == goal_data["match_id"]]
        match_start, match_end = match_data[["start_time", "end_time"]].values[0]
        # Discard goals that happened before and after the game
        time_check =  (goal_time >= match_start) and (goal_time <= match_end)
        return time_check
    except:
        # Discard goals for which match data wasn't found
        return False


def clean_data(df, config):
    event_type_col = config["event_type_column"]
    event_types = config["event_types"]
    # Required data columns for event of each type
    event_data = config["event_data"]

    df = df.drop_duplicates(subset=config["id_column"], keep="first", inplace=False)

    # Extract the dataframes for separate event types
    df_goals = df[df[event_type_col] == config[event_types["goal"]]].copy()
    df_match_start= df[df[event_type_col] == config[event_types["match_start"]]].copy()
    df_match_end = df[df[event_type_col] == config[event_types["match_end"]]].copy()

    df_goals = unflatten_event_data(df_goals, event_data["goal"])
    df_match_start = unflatten_event_data(df_match_start, event_data["match_start"])
    df_match_end = unflatten_event_data(df_match_end, event_data["match_end"])
    # Discard events which don't have all of the required columns
    df_goals = df_goals.dropna()
    df_match_start = df_match_start.dropna()
    df_match_end = df_match_end.dropna()

    # Unify the match data into a single dataframe
    df_matches = join_match_data(df_match_start, df_match_end, config["join_column"])

    # Discard matches with invalid start and end time
    df_matches = df_matches[df_matches["end_time"] > df_matches["start_time"]].copy()

    # Discard invalid goals
    df_goals = df_goals[df_goals.apply(filter_goals, axis=1)]