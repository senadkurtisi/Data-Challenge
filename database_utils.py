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
    query_create_table = f"CREATE TABLE IF NOT EXISTS clubs (club_name, league_id, points, goal_difference)"
    cursor.execute(query_create_table)
    db_conn.commit()

    # Dump the club stats to the database table
    df_stats.to_sql("clubs", db_conn, if_exists="replace", index=False)
