from argparse import ArgumentParser
import json

import sqlite3

from utils import load_dataset, clean_data, process_club_stats
from database_utils import save_club_stats


def parse_args():
    parser = ArgumentParser(description="Loads and cleans the dataset. Saves processed data to a database.")
    parser.add_argument("-d", "--dataset_path", type=str,
                        help="Path where the '.jsonl' dataset file is stored.")
    args = parser.parse_args()
    return args


def main():
    # Retrieve command line arguments
    args = parse_args()

    config_file_path = ".\\config.json"
    with open(config_file_path, "r") as cfg_file:
        config = json.load(cfg_file)

    # Open connection to the database
    db_conn = sqlite3.connect(config["database_name"])


    df_dataset = load_dataset(args.dataset_path)
    df_matches, df_goals = clean_data(df_dataset, config)
    club_stats = process_club_stats(df_matches, df_goals, config["match_id_column"])

    save_club_stats(club_stats, db_conn)
    db_conn.close()


if __name__ == "__main__":
    main()