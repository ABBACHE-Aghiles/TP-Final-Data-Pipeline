import argparse
import pandas as pd
from flask import Flask, request, jsonify
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Créer un objet ArgumentParser
parser = argparse.ArgumentParser(description='Lecture des fichiers csv')

# Ajouter un argument pour le chemin vers la BDD et le chemin vers le fichier CSV
parser.add_argument('database_uri', help='Chemin vers la BDD')
parser.add_argument('origin_data_path', help='Path to the CSV file to extract the data from')

# Analyser les arguments de la ligne de commande
args = parser.parse_args()

# Créer le DataFrame à partir du fichier CSV
sorted_users_df = pd.read_csv(args.origin_data_path)

# Créer la classe pour la table du leaderboard
Base = declarative_base()

class RankUser(Base):
    __tablename__ = 'rank_user'
    user_id = Column(Integer, primary_key=True)
    message_count = Column(Integer)
    first_name = Column(String)
    last_name = Column(String)

# Créer le moteur de base de données
engine = create_engine('sqlite:///' + args.database_uri, echo=True)
Base.metadata.create_all(engine)

# Créer la session
Session = sessionmaker(bind=engine)
session = Session()

# Parcourir le DataFrame et ajouter les informations à la table
for index, row in sorted_users_df.iterrows():
    rank_user = RankUser(
        user_id=row["user_id"],
        message_count=row["message_count"],
        first_name=row["first_name"],
        last_name=row["last_name"]
    )
    session.add(rank_user)

# Commit des modifications
session.commit()

# Fermeture de la session
session.close()

# Initialiser l'application Flask
app = Flask(__name__)

@app.route('/feed', methods=['POST'])
def feed_data():
    data_path = request.json['data_path']
    sorted_users_df = pd.read_csv("pipeline_result.csv")

    Base = declarative_base()

    class RankUser(Base):
        __tablename__ = 'rank_user'
        user_id = Column(Integer, primary_key=True)
        message_count = Column(Integer)
        first_name = Column(String)
        last_name = Column(String)

    engine = create_engine('sqlite:///AWS.db', echo=True)
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    for index, row in sorted_users_df.iterrows():
        rank_user = RankUser(
            user_id=row["user_id"],
            message_count=row["message_count"],
            first_name=row["first_name"],
            last_name=row["last_name"]
        )
        session.add(rank_user)

    session.commit()
    session.close()

    response = {'message': 'Data successfully fed into the database'}
    return jsonify(response), 200

@app.route('/leaderboard', methods=['GET'])
def get_leaderboard():
    engine = create_engine('sqlite:///AWS.db', echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()

    leaderboard = session.query(RankUser).order_by(RankUser.message_count.desc()).all()
    results = []

    for user in leaderboard:
        results.append({
            'user_id': user.user_id,
            'message_count': user.message_count,
            'first_name': user.first_name,
            'last_name': user.last_name
        })

    session.close()
    return jsonify(results), 200

@app.route('/feed/s3', methods=['POST'])
def process_s3_feed():
    # Check if the request is JSON
    if not request.is_json:
        return jsonify({'error': 'Invalid JSON payload'}), 400

    # Get the 's3_bucket' field from the JSON payload
    s3_bucket = request.json.get('s3_bucket')

    # Check if 's3_bucket' field exists
    if not s3_bucket:
        return jsonify({'error': 'Missing s3_bucket field'}), 400



    # Placeholder response
    return jsonify({'message': 'Processing S3 feed', 's3_bucket': s3_bucket})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)
