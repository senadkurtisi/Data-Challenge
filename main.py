import json

import sqlite3

from utils import load_dataset, clean_data, process_club_stats
from database_utils import save_club_stats


def main():
    config_file_path = ".\\config.json"
    with open(config_file_path, "r") as cfg_file:
        config = json.load(cfg_file)

    db_config = config["database_config"]
    # Open connection to the database
    db_conn = sqlite3.connect(db_config["database_name"])


    df_dataset = load_dataset(config["dataset_path"])
    df_matches, df_goals = clean_data(df_dataset, config)
    club_stats = process_club_stats(df_matches, df_goals, config["match_id_column"])

    save_club_stats(club_stats, db_conn)

    db_conn.close()


if __name__ == "__main__":
    main()