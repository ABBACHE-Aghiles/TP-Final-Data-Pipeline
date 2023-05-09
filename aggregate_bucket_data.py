import boto3
import pandas as pd

unique_name = "69"
access_key = "AKIAXEFYFAJHDPSHXSPP"
secret_key = "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"
destination_bucket_name= f'{unique_name}-result-data-bucket-md4-api'
# Créez un client S3
s3 = boto3.client('s3',
                  aws_access_key_id=access_key,
                  aws_secret_access_key=secret_key)

# Créez un nom de seau unique
bucket_name = f"{unique_name}-raw-data-bucket-md4-api"

# Créez le seau S3
s3.create_bucket(Bucket=bucket_name)

# Chargez messages.csv
with open("messages.csv", "rb") as file:
    s3.upload_fileobj(file, bucket_name, "messages.csv")

# Chargez users.csv
with open("users.csv", "rb") as file:
    s3.upload_fileobj(file, bucket_name, "users.csv")

# Read messages.csv from the source bucket
messages_obj = s3.get_object(Bucket=bucket_name, Key="messages.csv")
messages_df = pd.read_csv(messages_obj['Body'])

# Read users.csv from the source bucket
users_obj = s3.get_object(Bucket=bucket_name, Key="users.csv")
users_df = pd.read_csv(users_obj['Body'])

# Grouper les messages par utilisateur et compter le nombre de messages
user_message_counts = messages_df.groupby("author_id")["message_id"].count()

# Joindre les données des utilisateurs et les nombres de messages dans un dataframe unique
user_counts_df = pd.DataFrame({'user_id': user_message_counts.index, 'message_count': user_message_counts.values})
user_counts_df = pd.merge(user_counts_df, users_df, on='user_id')


# Trier les utilisateurs par ordre décroissant du nombre de messages
sorted_users_df = user_counts_df.sort_values(by=['message_count', 'user_id'], ascending=[False, True])

# Convert the merged DataFrame to a CSV file in memory
csv_buffer = sorted_users_df.to_csv(index=False)
# Create the destination bucket
s3.create_bucket(Bucket=destination_bucket_name)
# Upload the new CSV file to the destination bucket
s3.put_object(Body=csv_buffer, Bucket=destination_bucket_name, Key="pipeline_result.csv")

print("Data manipulation and storage completed successfully!")