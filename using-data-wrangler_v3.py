import awswrangler as wr
import time
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import json
from validations_json_with_units_cur_data_optimized_v3 import generate_json_data
import requests


# Generate the current month in "YYYY-MM-DD" format
# current_month = datetime.now().strftime("%Y-%m-01")
# last_month = (datetime.now() - relativedelta(months=1)).strftime("%Y-%m-01")

# # Set up the data payload with the current month
# data = {
#     "billing_period": last_month
# }

# # URL for the POST request
# url = "http://44.229.219.3:2525/get_no_bill_accounts"

# # Send the POST request
# response = requests.post(url, json=data)

# # Check the response
# if response.status_code == 200:
#     response_data = response.json()
#     account_ids = response_data["message"]
#     print(account_ids)
#     print("Response:", response.json())
# else:
#     print("Failed to fetch data:", response.status_code, response.text)
    
    
def get_last_month_info():
    current_date_utc = datetime.now(timezone.utc)
    last_month = current_date_utc - relativedelta(months=1)
    last_month_str = last_month.strftime('%Y-%m')
    year = str(last_month.year)
    month = str(int(last_month.strftime('%m')))
    last_month_str_with_start_date = f"{last_month_str}-01"
    return last_month_str, year, month,last_month_str_with_start_date

def build_queries(line_item_usage_account_id, month, year, last_month_str):
    billing_cur_query = f"""
    SELECT line_item_usage_account_id, line_item_line_item_description, line_item_product_code, 
           line_item_usage_amount, bill_payer_account_id, line_item_usage_start_date, 
           line_item_usage_end_date, bill_billing_entity, line_item_usage_type, 
           line_item_line_item_type, product_servicecode, product_instance_type, 
           product_product_family, line_item_unblended_cost, line_item_blended_cost, 
           pricing_unit, product_from_location, product_to_location, product_location 
    FROM aws_cur_2_0_billing_group 
    WHERE line_item_usage_account_id = '{line_item_usage_account_id}' 
      AND month = '{month}' AND year = '{year}'
    """
    
    cur_query = f"""
    SELECT line_item_usage_account_id, line_item_line_item_description, line_item_product_code, 
           line_item_usage_amount, bill_payer_account_id, line_item_usage_start_date, 
           line_item_usage_end_date, bill_billing_entity, line_item_usage_type, 
           line_item_line_item_type, product_servicecode, product_instance_type, 
           product_product_family, line_item_unblended_cost, line_item_blended_cost, 
           pricing_unit, product_from_location, product_to_location, product_location 
    FROM aws_cur_2_0
    WHERE line_item_usage_account_id = '{line_item_usage_account_id}'  
      AND billing_period = '{last_month_str}'
    """
    
    return billing_cur_query, cur_query

def process_account_data():
    last_month_str, year, last_month,last_month_str_with_date = get_last_month_info()
    data = {
        "billing_period": last_month_str_with_date
    }

    # URL for the POST request
    url = "http://44.229.219.3:2525/get_no_bill_accounts"

    # Send the POST request
    response = requests.post(url, json=data)

    # Check the response
    if response.status_code == 200:
        response_data = response.json()
        account_ids = response_data["message"]
        print(account_ids)
        print("Response:", response.json())
    else:
        print("Failed to fetch data:", response.status_code, response.text)    
        
    # account_ids = [{"bill_payer_account_id":"088415856281","account_id":"234702152893"}]
    account_ids = [{"bill_payer_account_id":"364437278543","account_id":"364437278543"},{"bill_payer_account_id":"088415856281","account_id":"701980022429"},{"bill_payer_account_id":"088415856281","account_id":"701980022429"},{"bill_payer_account_id":"088415856281","account_id":"234702152893"}]

    for account in account_ids:
        # bill_payer_account_id = account["bill_payer_account_id"]
        line_item_usage_account_id = account["account_id"]
        
        billing_cur_query, cur_query = build_queries(line_item_usage_account_id, last_month, year, last_month_str)
        
        try:
            cur_input = {"account_type": "cur", "account_id": line_item_usage_account_id, "query": cur_query}
            billing_cur_input = {"account_type": "billing_group", "account_id": line_item_usage_account_id, "query": billing_cur_query}
            
            input_data = [billing_cur_input, cur_input]
            
            # Execute JSON data generation
            # generate_json_data(input_data)
            apidata = generate_json_data(input_data)
            url = "http://44.229.219.3:2525/post_bill_summary"
            response = requests.post(url, json=apidata)
            # Check the response
            if response.status_code == 200:
                response_data = response.json()
                print("Message:", response_data.get("message", "No message"))
                print("Response:", response_data)
            else:
                print("Failed to post data:", response.status_code, response.text)
                
        except Exception as e:
            print(f"Error processing account {line_item_usage_account_id} : {e}")

# Start timing
start_time = time.time()

# Test data
# result = [
#     {"bill_payer_account_id": "364437278543", "line_item_usage_account_id": "364437278543"},
#     {"bill_payer_account_id": "088415856281", "line_item_usage_account_id": "701980022429"},
#     {"bill_payer_account_id": "088415856281", "line_item_usage_account_id": "234702152893"}
# ]

# Run the processing function
process_account_data()

# End timing
end_time = time.time()
print(f"Total execution time: {end_time - start_time} seconds")
