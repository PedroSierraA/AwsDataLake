import boto3
import json
import re
import logging

sf = boto3.client('stepfunctions')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # Extraer bucket y key del archivo subido
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    input_path = f"s3://{bucket}/{key}"

    logger.info(f"Archivo cargado: {input_path}")

    match = re.search(r"(\d{4})-(\d{2})", key)
    if match:
        year = match.group(1)
        month = str(int(match.group(2)))  
    else:
        year = None
        month = None

    response = sf.start_execution(
        stateMachineArn='arn:aws:states:us-east-2:474668398264:stateMachine:taxi_file_processor',
        input=json.dumps({
            'input_path': input_path,  # para el job raw
            'year': year,              # para el job cleaned
            'month': month
        })
    )
    return response
