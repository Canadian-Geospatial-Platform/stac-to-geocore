
import boto3
import logging 
from botocore.exceptions import ClientError
import pandas as pd 
import io
import os 

file_name = "records.parquet"
bucket_name = "webpresence-geocore-geojson-to-parquet-dev"


# Change directory 
# Get the current working directory
current_directory = os.getcwd()
print(f"Current Directory: {current_directory}")

# Change the working directory
new_directory = " "  # Replace with the directory you want to switch to
os.chdir(new_directory)
current_directory = os.getcwd()
print(f"New Directory: {current_directory}")


# Function to open a S3 file from bucket and filename and return the parquet as pandas dataframe
def open_S3_file_as_df(bucket_name, file_name):
    """Open a S3 parquet file from bucket and filename and return the parquet as pandas dataframe
    :param bucket_name: Bucket name
    :param file_name: Specific file name to open
    :return: body of the file as a string
    """
    try: 
        s3 = boto3.resource('s3')
        object = s3.Object(bucket_name, file_name)
        body = object.get()['Body'].read()
        df = pd.read_parquet(io.BytesIO(body))
        print(f'Loading {file_name} from {bucket_name} to pandas dataframe')
        return df
    except ClientError as e:
        logging.error(e)
        return e
    
df = open_S3_file_as_df(bucket_name, file_name)
print(f'The shape of the raw metadata parquet dataset is {df.shape}')

"""
# Add a new column to log the process, and loop through the pandas rows to assign values  
df['process_log'] = ''

## Loop through the DataFrame and update the new column based on processing condition 'Fail' or 'Success'
for index, row in df.iterrows():
    if Transfromed == True:
        df.at[index, 'process_log'] = 'Success' # or 1 
    else:
        df.at[index, 'process_log'] = 'Fail' # or 0
""" 
# Save all the records as a CSV to local path 
save_path = os.path.join(os.getcwd(), 'records.csv')
df.to_csv(save_path)
