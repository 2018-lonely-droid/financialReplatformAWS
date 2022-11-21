import json
import urllib3
import boto3
from faker import Faker
import datetime
import random
import uuid
fake = Faker()


banks = []
customers = []
accounts = []
transactions = []


def create_bank(row_count):
    row_count = int(row_count)
    for i in range(0, row_count):
        # Create bank records
        bank = {}
        bank['payloadType'] = 'bank'
        bank['bban'] = fake.bban()
        bank['swift'] = fake.swift8()
        bank['name'] = fake.name() + ' Bank'
        bank['address'] = fake.address()
        bank['phone'] = fake.msisdn()
        banks.append(bank)


def create_customer(row_count):
    row_count = int(row_count)
    for i in range(0, row_count):
        bank = random.choice(banks) # Pick random bank
        customer = {} # Create customer records
        customer['payloadType'] = 'customer'
        customer['bban'] = bank['bban']
        customer['customerID'] = str(uuid.uuid4())
        customer['firstName'] = fake.first_name()
        customer['lastName'] = fake.last_name() 
        customer['city'] = fake.city()
        customer['phone'] = fake.msisdn()
        customer['type'] = 'customer'
        customers.append(customer)


def create_account(row_count):
    row_count = int(row_count)
    for i in range(0, row_count):
        # Pick random customer
        customer = random.choice(customers)

        # Pick a weighted random account balance
        x = random.choices([100000, 500000, 2000000, 500000, 100000, 100000000], [.3, .2, .2, .1, .15, .05], k=1)

        account = {}
        account['payloadType'] = 'account'
        account['bban'] = customer['bban']
        account['customerID'] = customer['customerID']
        account['accountID'] = str(uuid.uuid4())

        # Account type
        x = random.choices(['Checking', 'Savings'], [.5, .5], k=1)
        account['type'] = x[0]

        # Change weighted balance depending on account type
        if account['type'] == 'Checking':
            # Pick a weighted random account balance -- Checking
            x = random.choices([10000, 50000, 200000, 300000, 400000, 500000], [.1, .2, .3, .2, .1, .1], k=1)
        else:
            # Pick a weighted random account balance -- Savings
            x = random.choices([100000, 500000, 2000000, 500000, 100000, 100000000], [.3, .2, .2, .1, .15, .05], k=1)

        account['balance'] = float(random.randrange(100, int(x[0])))/100

        # Status type
        x = random.choices(['active', 'closed'], [.9, .1], k=1)
        account['status'] = x[0]
        
        account['type'] = 'account'
        accounts.append(account)


def create_transaction(row_count):
    # List of industries taken from https://www.bls.gov/iag/tgs/iag_index_alpha.htm
    industry = ['Accommodation', 'Accommodation and Food Services', 'Administrative and Support Services', 'Administrative and Support and Waste Management and Remediation Services', 'Agriculture', 'Forestry, Fishing and Hunting', 'Air Transportation', 'Ambulatory Health Care Services', 'Amusement, Gambling, and Recreation Industries', 'Animal Production', 'Apparel Manufacturing', 'Arts, Entertainment, and Recreation', 'Beverage and Tobacco Product Manufacturing', 'Broadcasting, Building Material and Garden Equipment and Supplies Dealers', 'Chemical Manufacturing', 'Clothing and Clothing Accessories Stores', 'Computer and Electronic Product Manufacturing', 'Construction', 'Construction of Buildings', 'Couriers and Messengers', 'Credit Intermediation and Related Activities', 'Crop Production', 'Data Processing, Hosting, and Related Services', 'Education and Health Services', 'Educational Services', 'Electrical Equipment, Appliance, and Component Manufacturing', 'Electronics and Appliance Stores', 'Fabricated Metal Product Manufacturing', 'Finance and Insurance', 'Financial Activities', 'Fishing, Hunting and Trapping', 'Food Manufacturing', 'Food Services and Drinking Places', 'Food and Beverage Stores', 'Forestry and Logging', 'Funds, Trusts, and Other Financial Vehicles', 'Furniture and Home Furnishings Stores', 'Furniture and Related Product Manufacturing', 'Gasoline Stations', 'General Merchandise Stores', 'Goods-Producing Industries, Health Care and Social Assistance', 'Health and Personal Care Stores', 'Heavy and Civil Engineering Construction', 'Hospitals', 'Information', 'Insurance Carriers and Related Activities', 'Internet Publishing and Broadcasting', 'Leather and Allied Product Manufacturing', 'Leisure and Hospitality, Lessors of Nonfinancial Intangible Assets, Machinery Manufacturing', 'Management of Companies and Enterprises', 'Manufacturing', 'Merchant Wholesalers, Durable Goods', 'Merchant Wholesalers, Nondurable Goods', 'Mining, Mining, Quarrying, and Oil and Gas Extraction', 'Miscellaneous Manufacturing', 'Miscellaneous Store Retailers', 'Monetary Authorities - Central Bank', 'Motion Picture and Sound Recording Industries', 'Motor Vehicle and Parts Dealers', 'Museums, Historical Sites, and Similar Institutions', 'Natural Resources and Mining, Nonmetallic Mineral Product Manufacturing', 'Nonstore Retailers', 'Nursing and Residential Care Facilities', 'Oil and Gas Extraction', 'Other Information Services', 'Other Services, Paper Manufacturing', 'Performing Arts, Spectator Sports, and Related Industries', 'Personal and Laundry Services', 'Petroleum and Coal Products Manufacturing', 'Pipeline Transportation', 'Plastics and Rubber Products Manufacturing', 'Postal Service', 'Primary Metal Manufacturing', 'Printing and Related Support Activities', 'Private Households', 'Professional and Business Services, Professional, Scientific, and Technical Services', 'Publishing Industries, Rail Transportation', 'Real Estate', 'Real Estate and Rental and Leasing', 'Religious, Grantmaking, Civic, Professional, and Similar Organizations', 'Rental and Leasing Services', 'Repair and Maintenance', 'Retail Trade', 'Scenic and Sightseeing Transportation', 'Securities, Commodity Contracts, and Other Financial Investments and Related Activities', 'Service-Providing Industries', 'Social Assistance', 'Specialty Trade Contractors', 'Sporting Goods, Hobby, Book, and Music Stores', 'Support Activities for Agriculture and Forestry', 'Support Activities for Mining', 'Support Activities for Transportation', 'Telecommunications', 'Textile Mills', 'Textile Product Mills', 'Trade, Transportation, and Utilities', 'Transit and Ground Passenger Transportation', 'Transportation Equipment Manufacturing', 'Transportation and Warehousing', 'Truck Transportation']

    row_count = int(row_count)
    for i in range(0, row_count):
        # Pick random account
        account = random.choice(accounts)

        # Pick a weighted random transaction amount
        x = random.choices([10000, 50000, 200000, 300000, 400000, 500000], [.3, .3, .2, .1, .05, .05], k=1)

        transaction = {}
        transaction['payloadType'] = 'transaction'
        transaction['transactionID'] = str(uuid.uuid4())
        transaction['accountID'] = account['accountID']

        # Debit or credit
        x = random.choices(['debit', 'credit'], [.5, .5], k=1)
        transaction['type'] = x[0]

        x = random.choices([10000, 50000, 200000, 300000, 400000, 500000], [.1, .2, .3, .2, .1, .1], k=1)
        transaction['amount'] = float(random.randrange(100, int(x[0])))/100

        # Add a random agency
        x = random.choices(industry, weights=None, k=1)
        transaction['industry'] = x[0]

        transactions.append(transaction)


def lambda_handler(event, context):
    create_bank(1)
    create_customer(2)
    create_account(2)
    create_transaction(2000)
    
    bnk_cust_accnt_trans = [] # List for all records
    bnk_cust_accnt_trans = banks + customers + accounts + transactions
    
    random.shuffle(bnk_cust_accnt_trans) # Shuffle all data types to simulate random changes
    
    # Loop through all transactions, accounts, and customers, posting each one at a time.
    for row in bnk_cust_accnt_trans:
        partition =  {
            'PartitionKey':  str(uuid.uuid4()),
            'Data': row
        }
    
        url = 'https://92veyqxcx6.execute-api.us-east-1.amazonaws.com/alpha/streams/abfindata-encrypted/record?Action=PutRecord'
        for i in range(0, 1):
            # Post to kinesis data stream
            http = urllib3.PoolManager()
            resp = http.request("PUT", url, body=json.dumps(partition), headers={"Content-Type": "application/json"})
            print(resp.status, json.loads(resp.data.decode('utf-8'))) # Log Response