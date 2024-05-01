# Project Discription

This projects uses python to create an Extract,Transform and Load (ETL) pipeline by extracting a REST API json data from RapidAPI and tranform the data and Load it into a PostGresSQL database. For this example, we extract the top 20 football player from API-Football(link:https://rapidapi.com/api-sports/api/api-football).

This project uses libraries such as os,pandas,psycopg2,request and dotenv which we would go through it uses in this project.

## File structure

This project has 2 python script and an env file to load the sensitive variable. The python file `functions` has the helper functions which is used in the `main` python file. In the main python file, the environment variable are loaded and you are able to see the ETL pipeline.

## Library Used

`dotenv` and `os` library is used to store sensitive data(in our .env file) which we would later load onto our python script.

`request` is used to get the json response from RapidAPI.

`json` is used to parse json response and extract the needed data.

`pandas` is used to create a dataframe of the our dictionary and clean/sort our data by total goals and assist.

`psycopg2` is used to connect to our PostGresSQL database, create our table and insert the data.

## Fetch your own data

In order to tryout this code, you could clone this repo and install its dependency. Create a rapidapi account to and get the free API Key(https://rapidapi.com/api-sports/api/api-football). Also ensure you have postgres installed in your machine and fill in the host,username,password and dbname in the .env file. You should be able to see the result of the data from the ETL pipeline in your database. After installing the dependancy,updating the .env file and running the python script(in terminal, run `python main.py`) you should be able to see the top 20 soccer player data in your PG database.
