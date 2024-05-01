import os
from dotenv import load_dotenv
from functions import *

# Load environment variables from .env file
load_dotenv()

# Load API key to make API requests
RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY')

# Set up API request headers to authenticate requests
headers = {
    'X-RapidAPI-Key': RAPIDAPI_KEY,
    'X-RapidAPI-Host': 'api-football-v1.p.rapidapi.com'
}

HOST = os.getenv('HOST')
PG_DATABASE = os.getenv('PG_DATABASE')
PG_USERNAME = os.getenv('PG_USERNAME')
PG_PASSWORD = os.getenv('PG_PASSWORD')

# Set up API URL and parameters
url = "https://api-football-v1.p.rapidapi.com/v3/players/topscorers"
params = {"league":"39","season":"2023"}

def run_data_pipeline():
  """
  Execute the ETL pipeline 
  """

  check_rate_limits(url,headers)

  data = get_top_scorers(url, headers, params)

  if data and 'response' in data and data['response']:
    top_scorers = process_top_scorers(data)
    df = create_dataframe(top_scorers)
    print(df.to_string(index=False)) 

  else:
    print("No data available or an error occurred ❌")

  db_connection = create_db_connection(HOST, PG_USERNAME, PG_PASSWORD, PG_DATABASE)

  
  # If connection is successful, proceed with creating table and inserting data
  if db_connection is not None:
    create_table(db_connection)  
    df = create_dataframe(top_scorers) 
    insert_into_table(db_connection, df)  

if __name__ == "__main__":
    run_data_pipeline()