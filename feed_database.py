import pandas as pd
import argparse 

# Créer un objet ArgumentParser
parser = argparse.ArgumentParser(description='Lecture des fichiers csv')


# Ajouter un argument pour le nom du fichier
parser.add_argument('database_uri', help='Chemin vers la BDD')
parser.add_argument('origin_data_path', help='Path to the CSV file to extract the data from')
# Analyser les arguments de la ligne de commande
args = parser.parse_args()

sorted_users_df= pd.read_csv(args.origin_data_path)
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
engine = create_engine('sqlite:///'+ args.database_uri, echo=True)
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
