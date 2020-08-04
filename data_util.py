import boto3
import datetime
import pandas as pd
import time

def get_query_result(QueryString):
 
    athena = boto3.client('athena')
    s3 = boto3.client('s3')

    def execute_query(QueryString):

        ResultConfiguration = dict([])
        ResultConfiguration['OutputLocation'] = 's3://athena-output-cache/'
        response = athena.start_query_execution(QueryString=QueryString, ResultConfiguration=ResultConfiguration)
        QueryExecutionId = response['QueryExecutionId']

        flag = True
        while flag:
            response = athena.get_query_execution(QueryExecutionId=QueryExecutionId)
            if response['QueryExecution']['Status']['State'] == 'SUCCEEDED':
                flag = False
                OutputLocation = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                return OutputLocation
            elif response['QueryExecution']['Status']['State'] == 'FAILED':
                flag = False
                print(response['QueryExecution']['Status'])
                return
            else:
                time.sleep(0.5)

    output_location = execute_query(QueryString)
    response = s3.get_object(Bucket='athena-output-cache', Key=output_location.split('/')[-1])
    df = pd.read_csv(response['Body'])
    
    return df
