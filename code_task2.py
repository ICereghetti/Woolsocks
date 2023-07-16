import pandas as pd
import numpy as np
import os
from google.cloud import storage
from google.cloud import bigquery
from faker import Faker
import random
import pandas_gbq

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="credential.json"

###Store the file in a place where you can retrieve it via REST call (or do a manual dump into a database (like Google Bigquery)

client= storage.Client()

export_bucket = client.get_bucket('the-movie-database-project')

blob=export_bucket.blob('woolsocks/supermarket_bank_transactions.json')

blob.upload_from_filename(filename='WS_assignment_transactions_supermarket.json')

###Extract data from the shared CSV file and transform the data
  
#Download the file as a df
df = pd.read_json('gs://the-movie-database-project/woolsocks/supermarket_bank_transactions.json')

#I get all columns from the dictionary into the df
df=pd.merge(df,pd.json_normalize(df['Transaction']),
            left_index=True, 
            right_index=True).drop('Transaction',axis=True)

#Drop of duplicates
df=df.drop_duplicates()

#Dates into Bigquery's datetime format
df['RecordDate'] = pd.to_datetime(df['RecordDate']).astype('str')
df['UpdatedAtDate'] = pd.to_datetime(df['UpdatedAtDate']).astype('str')

# Identify the first occurrence of each customer in the database
df['db_customer_type'] = df.groupby('userId').cumcount() == 0
df['db_customer_type'] = df['db_customer_type'].map({True: 'New Customer', False: 'Frequent Customer'})

# Identify the first occurrence of each customer with a merchant
df['merchant_customer_type'] = df.groupby(['userId','MerchantName']).cumcount() == 0
df['merchant_customer_type'] = df['merchant_customer_type'].map({True: 'New Customer', False: 'Frequent Customer'})

#Replace empty for na
df.replace('', np.nan, inplace=True)

#Exploring data
df.describe()

###Load the transformed data into a database (for example Google Bigquery)

# Define the BigQuery data
project_id = 'the-movie-database-project'
dataset_id = 'woolsocks'

# desired table name
table_name = 'supermarket_bank_transactions'

pandas_gbq.to_gbq(df, f'{project_id}.{dataset_id}.{table_name}', project_id=project_id, if_exists='replace')

    


## Purchases by Cohort

query = """
CREATE OR REPLACE TABLE woolsocks.purchases_bycohort as
WITH
  first_purchases AS (
  SELECT
    userId,
    DATE(TIMESTAMP(RecordDate)) AS date,
    FIRST_VALUE(DATE(TIMESTAMP(RecordDate))) OVER (PARTITION BY userId ORDER BY DATE(TIMESTAMP(RecordDate))) AS first_purchase_date
  FROM woolsocks.supermarket_bank_transactions),
  
  month_date_agg AS (
  SELECT
    DATE_DIFF(date, first_purchase_date, MONTH) AS month_purchase,
    FORMAT_DATETIME('%Y%m', first_purchase_date) AS first_purchase,
    COUNT(DISTINCT userId) AS Customers
  FROM first_purchases
  GROUP BY first_purchase,month_purchase),

  cohort_definition AS (
  SELECT *,
    FIRST_VALUE(Customers) OVER (PARTITION BY first_purchase ORDER BY month_purchase) AS CohortCustomers
  FROM month_date_agg )

SELECT *,
  SAFE_DIVIDE(Customers, CohortCustomers) AS CohortCustomersPerc
FROM cohort_definition
ORDER BY first_purchase,month_purchase
"""

client = bigquery.Client(project=project_id)

query_results = client.query(query).result()


##Transition Matrix

top_4_supermarkets = df['MerchantName'].value_counts().nlargest(4).index.tolist()

df.loc[~df['MerchantName'].isin(top_4_supermarkets), 'MerchantName'] = 'Other'

# Group by consecutive purchases for each client
df['PreviousMerchant'] = df.groupby('userId')['MerchantName'].shift(1)

# Count the transitions from one supermarket to another
transition_matrix = df.groupby(['PreviousMerchant', 'MerchantName']).size().unstack(fill_value=0)

# Convert the counts to probabilities
transition_matrix = transition_matrix.div(transition_matrix.sum(axis=1), axis=0)

# Reset the index to convert the supermarket names into a column
transition_matrix = transition_matrix.reset_index()

# Reshape the DataFrame to long format
transition_matrix_long = transition_matrix.melt(id_vars='PreviousMerchant', var_name='MerchantName', value_name='Probability')

# Sort the DataFrame by 'PreviousSupermarket' and 'Supermarket' columns
transition_matrix_long = transition_matrix_long.sort_values(['PreviousMerchant', 'MerchantName'])

table_name='transition_matrix'

pandas_gbq.to_gbq(transition_matrix_long, f'{project_id}.{dataset_id}.{table_name}', project_id=project_id, if_exists='replace')

## Creo una base de datos falsa de clientes

# Initialize Faker
fake = Faker()

# Assuming you have a dataframe 'df' containing the original data
# Extract distinct values from the 'id' column
distinct_ids = df['userId'].unique()

# Generate fake client data with IDs
clients = []
for client_id in distinct_ids:
    client = {
        'id': client_id,
        'name': fake.name(),
        'country': random.choice(['Netherlands', 'Belgium', 'Germany']),
        'gender': random.choice(['Male', 'Female']),
        'age_group': random.choice(['18-24', '25-34', '35-44', '45-54', '55+']),
        'email': fake.email(),
        'phone_number': random.choice([fake.phone_number(),np.nan]),
        'address': random.choice([fake.address(),np.nan]),
        'occupation': random.choice([fake.job(),np.nan]),
        # Add more attributes as needed
    }
    clients.append(client)

clients_df=pd.DataFrame(clients)    


total_transactions = df.groupby('userId').size().reset_index(name='total_transactions')

# Get the place where clients made the most transactions
top_place = df.groupby('userId')['MerchantName'].apply(lambda x: x.value_counts().index[0]).reset_index(name='MerchantName')

# Merge total_transactions and top_place dataframes
summary_df = pd.merge(total_transactions, top_place, on='userId')

# Calculate the percentage of transactions in the top place
place_counts = df.groupby(['userId', 'MerchantName']).size().reset_index(name='place_counts')
summary_df = pd.merge(summary_df, place_counts, on=['userId', 'MerchantName'], how='left')
summary_df['percentage_in_top_place'] = (summary_df['place_counts'] / summary_df['total_transactions']) * 100

# Drop unnecessary columns
summary_df.drop(['place_counts'], axis=1, inplace=True)

# Merge the summary_df back to the original dataframe
clients_df = pd.merge(clients_df, summary_df, left_on='id',right_on='userId')


table_name='clients'

pandas_gbq.to_gbq(clients_df, f'{project_id}.{dataset_id}.{table_name}', project_id=project_id, if_exists='replace')
