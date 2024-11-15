import requests
from datetime import datetime
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
# Generate the current month in "YYYY-MM-DD" format
current_month = datetime.now().strftime("%Y-%m-01")

# Set up the data payload with the current month
data = {
    "billing_period": current_month
}

# URL for the POST request
url = "http://44.229.219.3:2525/get_no_bill_accounts"

# Send the POST request
response = requests.post(url, json=data)

# Check the response
if response.status_code == 200:
    response_data = response.json()
    account_ids = response_data["message"]
    # print(account_ids)
    print("Response:", response.json())
else:
    print("Failed to fetch data:", response.status_code, response.text)
current_date_utc = datetime.now(timezone.utc)
last_month = current_date_utc - relativedelta(months=1)
print(last_month,"last month")
last_month_str = last_month.strftime('%Y-%m')
print(last_month_str,"last month str")

year = str(last_month.year)
month = str(int(last_month.strftime('%m')))
print(month,"month")
last_month_str_with_start_date = f"{last_month_str}-01"
print(last_month_str_with_start_date,"last_month_str_with_start_date")
# return last_month_str, year, month,last_month_str_with_start_date