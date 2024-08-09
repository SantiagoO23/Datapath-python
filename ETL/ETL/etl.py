import pandas as pd
import sqlalchemy as db
from sqlalchemy import text
import pandas as pd
import json

with open("config.json") as file:
    config = json.load(file)

print(config)

engine = db.create_engine(f"mysql://root:root@{config['database']['host']}:{config['database']['port']}/db_movies_netflix_transact")
conn = engine.connect()

query = """
SELECT 
    movie.movieID as movieID, movie.movieTitle as title, movie.releaseDate as releaseDate, 
    gender.name as gender , person.name as participantName, participant.participantRole as roleparticipant 
FROM movie 
INNER JOIN participant 
ON movie.movieID=participant.movieID
INNER JOIN person
ON person.personID = participant.personID
INNER JOIN movie_gender 
ON movie.movieID = movie_gender.movieID
INNER JOIN gender 
ON movie_gender.genderID = gender.genderID
"""

movies_data=pd.read_sql(query, con=conn) 
movies_data["movieID"]=movies_data["movieID"].astype('int')
print(movies_data)
'''
movies_award=pd.read_csv("./data/Awards_movie.csv")
movies_award["movieID"]=movies_award["movieID"].astype('int')
movies_award.rename(columns={"Aware":"Award"}, inplace=True)
movies_award

movie_data=pd.merge(movies_data,movies_award, left_on="movieID", right_on="movieID")
movie_data

engine = db.create_engine("mysql://root:root@127.0.0.0:3310/dw_netflix")
conn = engine.connect()

movie_data = movie_data.rename(columns={'releaseDate': 'releaseMovie', 'Award': 'awardMovie'})

movie_data = movie_data.drop(columns=['IdAward'])

movie_data.to_sql('dimMovie',conn,if_exists='append', index=False)

users = pd.read_csv("./data/users.csv", sep='|')
users

users = users.rename(columns={'idUser': 'userID'})
users

users.to_sql('dimUser',conn,if_exists='append', index=False)

users_id=users["userID"]
movies_id=movies_data["movieID"]

watchs_data=pd.merge(users_id,movies_id, how="cross")
watchs_data

import random
from datetime import datetime, timedelta
import random

def gen_rating():
    # Generar un número aleatorio entre 0 y 5 con 1 solo decimal
    numero_aleatorio = round(random.uniform(0, 5), 1)
    # Mostrar el número aleatorio
    return numero_aleatorio

def gen_timestamp():
    # Generar un timestamp aleatorio dentro de un rango específico
    start_date = datetime(2024, 1, 15)
    end_date = datetime(2024, 4, 6)

    # Calcular un valor aleatorio entre start_date y end_date
    random_date = start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))

    # Mostrar el timestamp aleatorio
    return random_date

watchs_data["rating"]=watchs_data["movieID"].apply(lambda x: gen_rating())
watchs_data["timestamp"]=watchs_data["userID"].apply(lambda x: gen_timestamp())

watchs_data.to_sql("FactWatchs", conn, if_exists='append', index=False)
'''