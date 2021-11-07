import pandas as pd


def save_club_stats(club_stats, db_conn):
    """Saves club statistics in the database"""
    df_stats = pd.DataFrame(club_stats).transpose()
    # Line above has turned the column 'club_name' into the df index
    df_stats = df_stats.reset_index()
    # Reseting index doesn't restore the original column name
    df_stats = df_stats.rename(columns={"index": "club_name"})

    cursor = db_conn.cursor()

    # Create a table where club stats will be kept
    query_create_table = "CREATE TABLE IF NOT EXISTS clubs (club_name, league_id, points, goal_difference)"
    cursor.execute(query_create_table)
    db_conn.commit()

    # Dump the club stats to the database table
    df_stats.to_sql("clubs", db_conn, if_exists="replace", index=False)


def get_scoreboard(league_id, db_conn):
    """Retrieves scoreboard data.
    
    Rank is determined by three keys:
        1. Number of Points: Descending
        2. Goal difference: Descending
        3. Name: Ascending

    Returns:
        scoreboard_data (list of tuples): Each element of the tuple
            consists out of 3 elements - (club_name, points, goal_difference)
    """
    cursor = db_conn.cursor()
    query_get_scoreboard = """
        SELECT club_name, points, goal_difference
        FROM clubs WHERE league_id = ?
        ORDER BY points DESC, goal_difference DESC, club_name ASC
    """
    query_arguments = [league_id]
    cursor.execute(query_get_scoreboard, query_arguments)
    db_conn.commit()

    scoreboard_data = cursor.fetchall()
    return scoreboard_data
