import requests
from datetime import datetime
import pandas as pd
import psycopg2

def check_rate_limits(url,headers):
    """
    Check the API quota allocated to your account
    """
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    daily_limits = response.headers.get('x-ratelimit-requests-limit')
    daily_remaining = response.headers.get('x-ratelimit-requests-remaining')
    calls_per_min_allowed = response.headers.get('X-RateLimit-Limit')
    calls_per_min_remaining = response.headers.get('X-RateLimit-Remaining')
    
    rate_limits = {
        'daily_limit': daily_limits,
        'daily_remaining': daily_remaining,
        'minute_limit': calls_per_min_allowed,
        'minute_remaining': calls_per_min_remaining
    }
    print(rate_limits)

def get_top_scorers(url, headers, params):
    """
    Fetch the top scorers using the API 
    
    """
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    
    except requests.exceptions.HTTPError as http_error_message:
        print (f"❌ [HTTP ERROR]: {http_error_message}")
    
    except requests.exceptions.ConnectionError as connection_error_message:
        print (f"❌ [CONNECTION ERROR]: {connection_error_message}")
    
    except requests.exceptions.Timeout as timeout_error_message:
        print (f"❌ [TIMEOUT ERROR]: {timeout_error_message}")
    
    except requests.exceptions.RequestException as other_error_message:
        print (f"❌ [UNKNOWN ERROR]: {other_error_message}")


def process_top_scorers(data):
    """
    Parse the JSON data required for the top scorers 
    """
    top_scorers = []
    for scorer_data in data['response']:
        statistics = scorer_data['statistics'][0]

        # Set up constants for processing data 
        player = scorer_data['player']
        player_name = player['name']
        club_name = statistics['team']['name']
        total_goals = int(statistics['goals']['total'])
        penalty_goals = int(statistics['penalty']['scored'])
        assists = int(statistics['goals']['assists']) if statistics['goals']['assists'] else 0
        matches_played = int(statistics['games']['appearences'])
        minutes_played = int(statistics['games']['minutes'])
        dob = datetime.strptime(player['birth']['date'], '%Y-%m-%d')
        age = (datetime.now() - dob).days // 365

        # Append data 
        top_scorers.append({
            'player': player_name,
            'club': club_name,
            'total_goals': total_goals,
            'penalty_goals': penalty_goals,
            'assists': assists,
            'matches': matches_played,
            'mins': minutes_played,
            'age': age
        })
    return top_scorers

def create_dataframe(top_scorers):
    """
    Convert list of dictionaries into a Pandas dataframe and process it
    """

    df = pd.DataFrame(top_scorers)
    
    # Sort dataframe first by 'total_goals' in descending order, then by 'assists' in descending order
    df.sort_values(by=['total_goals', 'assists'], ascending=[False, False], inplace=True)
    
    # Reset index after sorting to reflect new order
    df.reset_index(drop=True, inplace=True)
    
    # Recalculate ranks based on the sorted order
    df['position'] = df['total_goals'].rank(method='dense', ascending=False).astype(int)
    
    # Specify the columns to include in the final dataframe in the desired order
    df = df[['position', 'player', 'club', 'total_goals', 'penalty_goals', 'assists', 'matches', 'mins', 'age']]
    
    return df

def create_db_connection(host_name, user_name, user_password, db_name):
    """
    Establish a connection to the MySQL database
    """
    connection = None
    try:
        connection = psycopg2.connect(database=db_name, user=user_name, password=user_password, host=host_name, port=5432)
        print("PostGresSQL Database connection successful ✅")

    except Exception as e:
        print(f"❌ [DATABASE CONNECTION ERROR]: '{e}'")

    return connection

def create_table(connection):
    """
    Create a table if it does not exist in the PostGresSQL database
    
    """
    
    CREATE_TABLE_SQL_QUERY = """
    CREATE TABLE IF NOT EXISTS top_scorers (
        position INT,
        player VARCHAR(255),
        club VARCHAR(255),
        total_goals INT,
        penalty_goals INT,
        assists INT,
        matches INT,
        mins INT,
        age INT,
        PRIMARY KEY (player, club)
    );
    """
    try:
        cursor = connection.cursor()
        cursor.execute(CREATE_TABLE_SQL_QUERY)
        connection.commit()
        print("Table created successfully ✅")

    except Exception as e:
        print(f"❌ [CREATING TABLE ERROR]: '{e}'")

def insert_into_table(connection, df):
    """
    Insert or update the top scorers data in the database from the dataframe
    """
    cursor = connection.cursor()

    INSERT_DATA_SQL_QUERY = """
    INSERT INTO top_scorers (position, player, club, total_goals, penalty_goals, assists, matches, mins, age)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (player, club) DO UPDATE SET
        (total_goals,penalty_goals,assists,matches,mins,age) = (EXCLUDED.total_goals,EXCLUDED.penalty_goals,EXCLUDED.assists,EXCLUDED.matches,EXCLUDED.mins,EXCLUDED.age)
    """
    # Create a list of tuples from the dataframe values
    data_values_as_tuples = [tuple(x) for x in df.to_numpy()]

    # Execute the query
    cursor.executemany(INSERT_DATA_SQL_QUERY, data_values_as_tuples)
    connection.commit()
    print("Data inserted or updated successfully ✅")