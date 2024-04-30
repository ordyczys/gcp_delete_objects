from google.cloud import storage
import subprocess
import csv

# set input CSV file
file_input = open('./input_files/bellator_purge_gcp_useast.csv', 'r')
file_input_reader = csv.reader(file_input, delimiter='|')

# set log file
file_log = open('./output_files/bellator_purge.csv', 'w')
file_log_writer = csv.writer(file_log, delimiter='|')

bucket_name = 'vmnalias-repo-2019-e7158178'
client = storage.Client()
bucket = client.bucket(bucket_name)


def list_objects(bucket,prefix):
    
    # set env
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=prefix, versions=True)

    for blob in blobs:
        file_log_writer.writerow([blob.name,blob.generation,blob.size])

def delete_gcp_object(bucket,gcp_name,gcp_generation):
    stats = storage.Blob(bucket=bucket, name=gcp_name, generation=gcp_generation).exists(client)
    if stats == True:
        print(f"Deleting - {gcp_name} {gcp_generation}")
        bucket.delete_blob(gcp_name, generation=gcp_generation) 
        file_log_writer.writerow([bucket_name, gcp_name, gcp_generation, "Deleted"])
    else:
        print(f"Doesn't exist - {gcp_name} {gcp_generation}")
        file_log_writer.writerow([bucket_name, gcp_name, gcp_generation, "Error"])

for row in file_input_reader:
    gcp_name = row[0]
    gcp_generation = row[1]
    delete_gcp_object(bucket,gcp_name,gcp_generation)

file_log.close()
file_input.close()
