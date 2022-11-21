import boto3
import json
import base64
import sys
import mysql.connector
import os
import logging
import time
from datetime import datetime


logger = logging.getLogger() # Declare logger
logger.setLevel(logging.INFO) # Set desired logging level
ssm_client = boto3.client('ssm') # Create our SSM client
lambda_client = boto3.client('lambda') # Create our lambda client
cloudwatch_client = boto3.client('logs') # Create our cloudwatch client


def get_password(name): # Gets & decrypts password from Parameetr Store
  response = ssm_client.get_parameter(Name=name, WithDecryption=True)
  return response['Parameter']['Value']
  
  
def get_sql(row):
  if row['payloadType'] == 'bank':
    sql =   """
            INSERT INTO abfindata.banks (bban, swift, name, address, phone)
            VALUES (%s, %s, %s, %s, %s)
        """
    val = (row['bban'], row['swift'], row['name'], row['address'], row['phone'])
    
  elif row['payloadType'] == 'customer':
    sql =   """
            INSERT INTO abfindata.customers (bban, customerID, firstName, lastName, city, phone)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
    val = (row['bban'], row['customerID'], row['firstName'], row['lastName'], row['city'], row['phone'])
    
  elif row['payloadType'] == 'account':
    sql =   """
            INSERT INTO abfindata.accounts (bban, customerID, accountID, type, balance, status)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
    val = (row['bban'], row['customerID'], row['accountID'], row['type'], row['balance'], row['status'])
    
  elif row['payloadType'] == 'transaction':
    sql =   """
            INSERT INTO abfindata.transactions (transactionID, accountID, type, amount, industry)
            VALUES (%s, %s, %s, %s, %s)
        """
    val = (row['transactionID'], row['accountID'], row['type'], row['amount'], row['industry'])
    
  else:
    logger.error('|GET_SQL|Unknown payload type error|', row['payloadType'], '|')
    
  return sql, val


def insert_event(row): # Inserts record event data into aurora
  sql, val = get_sql(row)
  logger.info('|INSERT_EVENT|SQL|' + str(sql) + '|')
  logger.info('|INSERT_EVENT|value|' + str(val) + '|')

  # Connect to DB
  mydb = mysql.connector.connect(
    host = 'abfindata.cluster-c32ax1pazlnq.us-east-1.rds.amazonaws.com',
    user = 'admin',
    password = get_password('abfindata-aurora-key'),
    database = 'abfindata')
  mycursor = mydb.cursor()
  
  # Try to insert row into correct table
  try:
    mycursor.execute(sql, val)
    mydb.commit()
    logger.info('|INSERT_EVENT|mysql insert|' + str(mycursor.rowcount) + ' record(s) inserted|')
  except:
    logger.error('|INSERT_EVENT|Unexpected mysql.connector error|' + str(sys.exc_info()[0]) + '|')
  finally:
    mycursor.close() # Always close db connection even if insert fails
    mydb.close()


def trigger_fraud_lambda(row):
  lambda_payload = json.dumps({"transactionID": row['transactionID'], "transactionAmount": row['amount'], "transactionIndustry": row['industry'], 'transactionAccountID': row['accountID']}).encode('utf-8')
  logger.info('|TRIGGER_FRAUD_LAMBDA|Potential fraudulent record found|' + str(row) + '|')
  lambda_client.invoke(FunctionName='finDataFraudPrevention', InvocationType='Event', Payload=lambda_payload)


def log_to_S3():
  # Get last export date held in parameter store
  try:
    lastExportResp = ssm_client.get_parameter(Name='finDataReciever_LastExport')
    lastExport = lastExportResp['Parameter']['Value']
  except ssm_client.exceptions.ParameterNotFound:
    lastExport = '2022-1-1'
    
  # Convert last export to date for comparison
  lastExportDate = datetime.strptime(lastExport, "%Y-%m-%d")
  presentDate = datetime.now()
  
  # If last export was not today, export yesterday's cloudwatch logs
  if lastExportDate.date() < presentDate.date():
    yesterdayStart = lastExport + ' 00:00:00'
    yesterdayStartDatetime = datetime.strptime(yesterdayStart, '%Y-%m-%d %H:%M:%S')
    yesterdayStartMil = int(yesterdayStartDatetime.timestamp() * 1000)
    yesterdayEndMil = int(yesterdayStartMil + 86400000)
    
    # Try to insert all logs from yesterday to S3
    try:
      response = cloudwatch_client.create_export_task(
          logGroupName='/aws/lambda/finDataReciever',
          fromTime=yesterdayStartMil,
          to=yesterdayEndMil,
          destination='findata-logs-3849302183',
          destinationPrefix='/finDataReciever_log/'
      )
      
      logger.info('|LOG_TO_S3|Task created: ' + str(response['taskId']) + '|Logging yesterday logs to S3|')
      
      # Wait for the log task to complete before continuing
      taskId = response['taskId']
      status = 'RUNNING'
      while status in ['RUNNING','PENDING']:
        response_desc = cloudwatch_client.describe_export_tasks(taskId=taskId)
        status = response_desc['exportTasks'][0]['status']['code']
        
    except logs.exceptions.LimitExceededException:
      logger.info('|LOG_TO_S3|Need to wait until all tasks are finished (LimitExceededException). Continuing later...|')
    
    # Log updated date value in parameter store
    ssm_response = ssm_client.put_parameter(
      Name='finDataReciever_LastExport',
      Type="String",
      Value=str(presentDate.date()),
      Overwrite=True)
    logger.info('|LOG_TO_S3|lastExport in KSM updated to ' + str(presentDate.date()) + '|')
      
  else:
    logger.info('|LOG_TO_S3|Export completed earlier today. No export needed until tomorrow|')


def lambda_handler(event, context):
  row = json.loads(base64.b64decode(event['Records'][0]["kinesis"]["data"]).decode("utf-8")) # Decode Kinesis Record
  insert_event(row) # Insert row into aurora db
  
  if row['payloadType'] == 'transaction': # Only trigger lambda if record was a transaction
    trigger_fraud_lambda(row) # Trigger fraud lambda with transaction ID
    
  log_to_S3() # Attempt to log yesterdays logs to S3