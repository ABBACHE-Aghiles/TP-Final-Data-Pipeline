import pandas as pd
import argparse 


# Créer un objet ArgumentParser
parser = argparse.ArgumentParser(description='Lecture des fichiers csv')


# Ajouter un argument pour le nom du fichier
parser.add_argument('users_csv', help='Chemin vers le fichier users.csv')
parser.add_argument('messages_csv', help='Chemin vers le fichier messages.csv')
parser.add_argument('csv_file', help='Nom du fichier CSV pour enregister le rank')
# Analyser les arguments de la ligne de commande
args = parser.parse_args()

# Lire les fichiers CSV dans des dataframes Pandas
users_df = pd.read_csv(args.users_csv)
messages_df = pd.read_csv(args.messages_csv)

# Grouper les messages par utilisateur et compter le nombre de messages
user_message_counts = messages_df.groupby("author_id")["message_id"].count()

# Joindre les données des utilisateurs et les nombres de messages dans un dataframe unique
user_counts_df = pd.DataFrame({'user_id': user_message_counts.index, 'message_count': user_message_counts.values})
user_counts_df = pd.merge(user_counts_df, users_df, on='user_id')


# Trier les utilisateurs par ordre décroissant du nombre de messages
sorted_users_df = user_counts_df.sort_values(by=['message_count', 'user_id'], ascending=[False, True])
print(sorted_users_df)


sorted_users_df.to_csv(args.csv_file, index=False)
print("sorted_users_df enregistré dans le fichier CSV :", args.csv_file)
