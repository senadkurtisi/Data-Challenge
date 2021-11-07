import json

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
