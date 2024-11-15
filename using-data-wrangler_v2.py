import awswrangler as wr
import pandas as pd
import time
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import json
import boto3
import urllib.parse
import os
import datetime as dt
import gc

from validations_json_with_units_cur_data_optimized_v3 import generate_json_data

current_date_utc = datetime.now(timezone.utc)
day_of_month = current_date_utc.day


# Calculate the previous month
last_month = current_date_utc - relativedelta(months=1)
last_month_str = last_month.strftime('%Y-%m')
year = last_month.year
year = str(year)
print(last_month_str)
# Get the month as an integer without leading zero
month = str(int(last_month.strftime('%m')) )
print(year,month)
month_start_date = f"{last_month_str}-01"
# Start timing
start = time.time()

result = [{"bill_payer_account_id":"088415856281","line_item_usage_account_id":"234702152893"}]
result = [{"bill_payer_account_id":"364437278543","line_item_usage_account_id":"364437278543"},{"bill_payer_account_id":"088415856281","line_item_usage_account_id":"701980022429"},{"bill_payer_account_id":"088415856281","line_item_usage_account_id":"701980022429"},{"bill_payer_account_id":"088415856281","line_item_usage_account_id":"234702152893"}]


# with open("result.json","r") as f:
#     result = json.loads(f.read())

# print(result)

# def generate_json(cur,billing_cur):
#     account_type = [cur,billing_cur]
#     for i in account_type:
#         if i["account_type"] =="cur":
            
#         if i["account_type"] =="billing_group":

for i in result:
    bill_payer_account_id = i["bill_payer_account_id"]
    line_item_usage_account_id = i["line_item_usage_account_id"]
    # billing_cur = pd.read_csv("701980022429.csv")
    billing_cur_query = f"""
    SELECT line_item_usage_account_id,line_item_line_item_description,line_item_product_code,line_item_usage_amount,bill_payer_account_id,line_item_usage_start_date,line_item_usage_end_date,bill_billing_entity,line_item_usage_type,line_item_line_item_type,product_servicecode,product_instance_type,product_product_family,line_item_unblended_cost,line_item_blended_cost,pricing_unit,product_from_location,product_to_location,product_location FROM aws_cur_2_0_billing_group 
    WHERE line_item_usage_account_id = '{line_item_usage_account_id}' 
    AND month = '{month}' and year = '{year}'
    """
    # cur =  pd.read_csv(r"c:\Users\user\Downloads\kpoint-test-account-from-cur.csv")
    cur_query = f"""
    SELECT line_item_usage_account_id,line_item_line_item_description,line_item_product_code,line_item_usage_amount,bill_payer_account_id,line_item_usage_start_date,line_item_usage_end_date,bill_billing_entity,line_item_usage_type,line_item_line_item_type,product_servicecode,product_instance_type,product_product_family,line_item_unblended_cost,line_item_blended_cost,pricing_unit,product_from_location,product_to_location,product_location FROM aws_cur_2_0
    WHERE line_item_usage_account_id = '{line_item_usage_account_id}'  and
    billing_period = '{last_month_str}'
    """
    # start = time.time()
    # billing_cur = wr.athena.read_sql_query(billing_cur_query, database="athena")
    # end = time.time()
    # print("time taken for billling_group:",end-start)
    # start = time.time()
    # cur = wr.athena.read_sql_query(cur_query, database="athena")
    # end = time.time()
    # print("time taken for aws_cur:",end-start)
    cur_input = {"account_type":"cur","accout_id":bill_payer_account_id,"query":cur_query} 
    billing_cur_input = {"account_type":"billing_group","accout_id":line_item_usage_account_id,"query":billing_cur_query}
    input = [billing_cur_input,cur_input]
    # input = [billing_cur_input]
    generate_json_data(input)
    # del cur,billing_cur
    # gc.collect()