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
