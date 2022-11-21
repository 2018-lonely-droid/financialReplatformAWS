import json
import base64
import boto3
import mysql.connector

# https://en.wikipedia.org/wiki/Benford%27s_law
#               0  1     2     3     4    5    6    7    8    9
BENFORDS_LAW = [0, 30.1, 17.6, 12.5, 9.7, 7.9, 6.7, 5.8, 5.1, 4.6]

ACCOUNT_HIGH_AMOUNT_THRESHOLD = 2

INDUSTRY_HIGH_AMOUNT_THRESHOLD = 3

BENFORDS_LAW_THRESHOLD_DIF = 7
BENFORDS_LAW_THRESHOLD_AMOUNT = 3
BENFORDS_LAW_MIN_TRANS_COUNT = 50

ssm_client = boto3.client('ssm')
def get_password(name): # Gets & decrypts password from Parameetr Store
  print('getting password')
  response = ssm_client.get_parameter(Name=name, WithDecryption=True)
  return response['Parameter']['Value']

def lambda_handler(event, context):
    tran_id = event['transactionID']
    tran_amount = event['transactionAmount']
    tran_industry = event['transactionIndustry']
    tran_account_id = event['transactionAccountID']
    
    mydb = mysql.connector.connect(
        host = "abfindata.cluster-c32ax1pazlnq.us-east-1.rds.amazonaws.com",
        user = "admin",
        password = get_password('abfindata-aurora-key'),
        database = "abfindata")
          
    sql_get_account_tran_amount_history = "SELECT amount FROM transactions WHERE accountID = '%s' AND transactionID != '%s'" % (tran_account_id, tran_id) # These might need limits
    print(sql_get_account_tran_amount_history)
    sql_get_industry_tran_amount_average = "SELECT AVG(amount) FROM transactions WHERE industry = '%s' AND transactionID != '%s'" % (tran_industry, tran_id)
    print(sql_get_industry_tran_amount_average)

    # select DB
    mycursor = mydb.cursor()
    # execute SQL
    # get recent transaction history for that account
    mycursor.execute(sql_get_account_tran_amount_history)
    account_tran_amount_history = mycursor.fetchall()
    # get average transaction size of that industry
    mycursor.execute(sql_get_industry_tran_amount_average)
    avg_industry_amount = mycursor.fetchall()
    # DB work is done
    
    #clean
    account_tran_history = []
    for tran in account_tran_amount_history:
        tran=float(tran[0])
        account_tran_history.append(tran)
    
    # do work
    # is this transaction over the past account transaction history average by a factor of _?
    flag_high_amount_account = False
    if len(account_tran_history) > 0:
       
        # find the average and compare
        sum = 0
        for tran in account_tran_history:
            sum+=tran
        avg_account_amount = sum/len(account_tran_history)
        print(avg_account_amount)
        if tran_amount > avg_account_amount*ACCOUNT_HIGH_AMOUNT_THRESHOLD:
            flag_high_amount_account = True
    else: 
        print('no baseline available for account transaction history')
     
    
    # is this transaction over the past industry transaction history average by a factor of _?
    flag_high_amount_industry = False
    if len(avg_industry_amount) > 0:
        avg_industry_amount = avg_industry_amount[0][0]
        if tran_amount > avg_industry_amount*INDUSTRY_HIGH_AMOUNT_THRESHOLD: 
            flag_high_amount_industry = True
    else : 
        print('no baseline available for industry transaction history')
    
    # Benfords Law, do past transactions, and especially this one, generally follow benfords law?
    TEST_account_tran_amount_history = [100, 200, 300, 100, 100]
    flag_account_benford = False
    if len(account_tran_history) >= BENFORDS_LAW_MIN_TRANS_COUNT:
        # map to a benfords law value
        benford_values = []
        for tran in account_tran_history:
            first_digit = int(str(tran)[:1])
            benford_value = BENFORDS_LAW[first_digit]
            benford_values.append(benford_value)
        
        current_benford_value = BENFORDS_LAW[int(str(tran_amount)[:1])]
        benford_values.append(current_benford_value)
        
        # make a benford score
        # make a map of digits and their counts, and then find the distribution and see how much it differs from benford.
        #                       0  1  2  3  4  5  6  7  8  9
        current_distribution = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        dif = []
        for tran in account_tran_history:
            first_digit = int(str(tran)[:1])
            current_distribution[first_digit] = current_distribution[first_digit]+1
        multiplier = 100/len(account_tran_history)
        benford_sum = 0
        for index in range(0, len(current_distribution)):
            adjusted_benford_value = current_distribution[index] * multiplier
            current_distribution[index] = adjusted_benford_value
            dif.append(abs(adjusted_benford_value - BENFORDS_LAW[index]))
            benford_sum = benford_sum + adjusted_benford_value
        threshold_count = 0
        for num in dif:
            if num > BENFORDS_LAW_THRESHOLD_DIF:
                threshold_count+=1
        if threshold_count > BENFORDS_LAW_THRESHOLD_AMOUNT or abs(100-benford_sum) > BENFORDS_LAW_THRESHOLD_DIF:
            flag_account_benford = True
        # print('Benford Sum (Should be 100):', benford_sum)
        # print(current_distribution)
        # print(BENFORDS_LAW)
        # print(dif)
        
    else:
         print('no baseline available for account transaction history; cannot test benfords law')
         
    # send notification if flagged
    if flag_high_amount_account or flag_high_amount_industry or flag_account_benford:
        print('sending flags to SNS')
        notification = 'Recent transaction with ID of %s, has been flagged for: ' % (tran_id)
        if flag_high_amount_account:
            # update notification
            notification = notification + '\nHigh amount based on previous transactions in the same account.'
            # update DB
            sql = """
                INSERT INTO abfindata.transactionFlags (flagID, transactionID)
                VALUES (%s, %s)
            """
            val = ('1', tran_id)
            mycursor.execute(sql, val)
            mydb.commit()
        
        if flag_high_amount_industry:
            notification = notification + '\nHigh amount based on previous transactions in the same industry.'
            # update DB
            sql = """
                INSERT INTO abfindata.transactionFlags (flagID, transactionID)
                VALUES (%s, %s)
            """
            val = ('2', tran_id)
            mycursor.execute(sql, val)
            mydb.commit()
        
        if flag_account_benford:
            notification = notification + "\nIts account's transactions do not follow Benford's law."
            # update DB
            sql = """
                INSERT INTO abfindata.transactionFlags (flagID, transactionID)
                VALUES (%s, %s)
            """
            val = ('3', tran_id)
            mycursor.execute(sql, val)
            mydb.commit()
            
        client = boto3.client('sns')
        response = client.publish(
            TargetArn = "arn:aws:sns:us-east-1:282048348547:FraudlentTransactions",
            Message = json.dumps({'default': notification}),
            MessageStructure = 'json'
        )
        
    mydb.close()
    
    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }