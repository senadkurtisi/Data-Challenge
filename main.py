import json
from argparse import ArgumentParser

import sqlite3

from utils.database_utils import get_scoreboard
from utils.utils import pretty_print_scoreboard


def parse_args():
    parser = ArgumentParser(description="Retrieves scoreboard for the desired league")
    required_args = parser.add_argument_group("Required arguments")
    required_args.add_argument("--league_id", type=int, required=True,
                              help="Id of a league for which we want to get the scoreboard.")
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

    # Get and display the scoreboard
    scoreboard_data = get_scoreboard(args.league_id, db_conn)
    pretty_print_scoreboard(scoreboard_data)
    
    db_conn.close()


if __name__ == "__main__":
    main()
