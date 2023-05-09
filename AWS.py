import pandas as pd
import argparse 
import csv
import sys

# Créer un objet ArgumentParser
parser = argparse.ArgumentParser(description='Lecture des fichiers csv')


# Ajouter un argument pour le nom du fichier
parser.add_argument('users_path', help='Chemin vers le fichier users.csv')
parser.add_argument('messages_path', help='Chemin vers le fichier messages.csv')
parser.add_argument('output_path', help='Nom du fichier CSV pour enregister le rank')
# Analyser les arguments de la ligne de commande
args = parser.parse_args()

# Lire les fichiers CSV dans des dataframes Pandas
users_df = pd.read_csv(args.users_path)
messages_df = pd.read_csv(args.messages_path)

# Grouper les messages par utilisateur et compter le nombre de messages
user_message_counts = messages_df.groupby("author_id")["message_id"].count()

# Joindre les données des utilisateurs et les nombres de messages dans un dataframe unique
user_counts_df = pd.DataFrame({'user_id': user_message_counts.index, 'message_count': user_message_counts.values})
user_counts_df = pd.merge(user_counts_df, users_df, on='user_id')


# Trier les utilisateurs par ordre décroissant du nombre de messages
sorted_users_df = user_counts_df.sort_values(by=['message_count', 'user_id'], ascending=[False, True])
print(sorted_users_df)


sorted_users_df.to_csv(args.output_path, index=False)
print("sorted_users_df enregistré dans le fichier CSV :", args.output_path)

from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class RankUser(Base):
    __tablename__ = 'rank_user'
    user_id = Column(Integer, primary_key=True)
    message_count = Column(Integer)
    first_name = Column(String)
    last_name = Column(String)

# create a database engine
engine = create_engine('sqlite:///AWS.db', echo=True)
Base.metadata.create_all(engine)

# create a session
Session = sessionmaker(bind=engine)
session = Session()

# parcourir le dataframe et ajouter les informations à la table
for index, row in sorted_users_df.iterrows():
    rank_user = RankUser(
        user_id=row["user_id"],
        message_count=row["message_count"],
        first_name=row["first_name"],
        last_name=row["last_name"]
    )
    session.add(rank_user)

# commit the changes
session.commit()

# close the session
session.close()
