import json
import boto3
import pandas as pd 
from datetime import datetime
from io import StringIO
import os

#boto3 client initiations
s3_client = boto3.client("s3")
sqs_client  = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb")
config_key = "config/dq_config.csv"

#dynamodb tables call
table1 = dynamodb.Table("project-staging_data")
table2 = dynamodb.Table("dq-check-statu-project")

#SQS URL
queue_url = "https://sqs.us-east-1.amazonaws.com/604371741748/bindu-batch7-success-queue-test"

def read_config(bucket_name):
    config_obj = s3_client.get_object(Bucket=bucket_name, Key=config_key)
    config_df = pd.read_csv(StringIO(config_obj['Body'].read().decode('utf-8')))
    return config_df["column_name"].tolist()
def validate_dq(df, required_columns):
    dq_results = {}
    row_count = len(df)
    dq_results["row_count"] = row_count
    dq_results["missing_columns"] = [col for col in required_columns if col not in df.columns]    
    dq_results['duplicate_rows'] = int(df.duplicated().sum())
    if 'id' in df.columns:
       dq_results['duplicate_ids'] = int(df['id'].duplicated().sum())
       dq_results['row_count_validation'] = row_count > 0
       return dq_results

def store_employee_data(table,employees,historical_flag,incremental_flag):
    for i in employees:
        if i.strip() == "":
            continue  
        employee_data = i.split(",")
        try:
           item = {
              'id': employee_data[0],
              'name': employee_data[1],
              'age': employee_data[2],
              'gender': employee_data[3],
              'department': employee_data[4],
              'position': employee_data[5],
              'salary': employee_data[6],
              'joining_date': employee_data[7],
              'experience_years': employee_data[8],
              'last_modified_timestamp': employee_data[9],
              'historical_flag' : historical_flag,
              'incremental_flag': incremental_flag
           }
           table.put_item(Item=item)
        except Exception as e:
            print("error in reading csv data", e)
            continue
def copy_file_to_stage(source_bucket,source_key,destination_bucket,file_name):
    copy_source = {'Bucket': source_bucket, 'Key': source_key}
    s3_client.copy_object(CopySource=copy_source,
                          Bucket=destination_bucket,
                          Key="Stagedata/" + file_name)
def read_csv_from_s3(bucket_name, key):
    s3_object = s3_client.get_object(Bucket=bucket_name, Key=key)
    body = s3_object['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(body))      
    return body, df  
def store_status(table, file_name, bucket_name, row_count, historical_flag,incremental_flag):   
    item = {
        'object': file_name,
        'status': 'success',
        'timestamp': str(datetime.now()),
        'bucketname': bucket_name,
        'row_count': int(row_count),
        'historical_flag': historical_flag,
        'incremental_flag': incremental_flag
    }   
    print("item is ", item)  
    table.put_item(Item=item)         
def lambda_handler(event, context):
    try:
        historical_flag = event.get("historical_flag", "N")
        incremental_flag = event.get("incremental_flag", "N")
        print("incremental flag value is", incremental_flag)
        print("historical flag value is", historical_flag)
        #s3 event invocations
        if "Records" in event:
            s3_bucket_name = event['Records'][0]['s3']['bucket']['name']
            s3_file_name = event['Records'][0]['s3']['object']['key']
        else:
            #api event invocation
            s3_bucket_name = event['bucketname']
            s3_file_name = event['file_name']
        file_name = s3_file_name.split("/")[-1]
        #s3_client.put_object(Bucket="binduprojectdata", Key = "Stagedata/")
        #reading csv file from s3 bucket
        copy_file_to_stage(s3_bucket_name, s3_file_name, "binduprojectdata", file_name)
        body, df = read_csv_from_s3("binduprojectdata", "Stagedata/" + file_name)
        df["historical_flag"] = historical_flag
        df["incremental_flag"] = incremental_flag
        required_columns = read_config(s3_bucket_name)
        #Prelimnary data checks
        print("data is available in csv",df.head())
        print("list of columns in csv", list(df.columns))

        #to fetch row counts
        row_count=len(df)
        print("row count of the data is", row_count)

        #handle DQ checks
        dq_results = validate_dq(df, required_columns)
        print("dq results are", dq_results)
        #storing csv data into dynmodb table

        employees = body.split("\n")
        employees.pop(0)
        store_employee_data(table1, employees, historical_flag,incremental_flag)
        store_status(table2, s3_file_name, s3_bucket_name, row_count, historical_flag,incremental_flag)        
        #add current timestamp for successful message to sqs queue
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = "DQ checks are passed & data inserted into dynamodb table" + "" + current_time
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket="binduprojectdata", Key="stagedata/" + file_name.split(".")[0] + ".csv", Body=csv_buffer.getvalue())
        response = sqs_client.send_message(QueueUrl=queue_url, MessageBody=message)
        return {
            'status': "SUCCESS",
            'bucketname': s3_bucket_name,
            'filename': file_name,
            'historical_flag': historical_flag,
            'incremental_flag': incremental_flag,
        }
    except Exception as e:
        print("error in lambda function", e)
        raise e
        
