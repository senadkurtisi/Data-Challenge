import json

from utils import load_dataset, clean_data, process_club_stats


def main():
    config_file_path = ".\\config.json"
    with open(config_file_path, "r") as cfg_file:
        config = json.load(cfg_file)

    df_dataset = load_dataset(config["dataset_path"])
    df_matches, df_goals = clean_data(df_dataset, config)
    club_stats = process_club_stats(df_matches, df_goals, config["match_id_column"])


if __name__ == "__main__":
    main()