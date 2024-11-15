import pandas as pd
import json
import numpy as np
import awswrangler as wr
# import pandas as pd
import time
import gc
from datetime import datetime
from json_to_pdf import jsonTOpdf


def find_highest_spends(data):
    services = data["billing_group"]["Charges by service"]
    highest_service = max(services, key=lambda x: x['service_total'])

    regions = []
    for service in services:
        regions.extend(service.get('regions', [])) 
    highest_region = max(regions, key=lambda x: x['region_total'])

    return highest_service, highest_region

def parse_usage_type(usage_type):
    """
    Parses the lineItem/UsageType field for Amazon EC2, including CPUCredits, EBSOptimized,
    and returns extracted information as a dictionary.
    """
    parts = usage_type.split("-")
    # print(parts)
    
    # Initialize result dictionary with default structure
    result = {
        'region': None,
        'usage_category': None,
        'instance_type_or_detail': None,
        'additional_info': None
    }

    # Handle NAT Gateway as a special case
    if 'NatGateway' in usage_type:
        if len(parts) == 3:  # e.g., APS3-NatGateway-Bytes
            result['region'] = parts[0]
            result['usage_category'] = 'NatGateway'
            result['instance_type_or_detail'] = parts[2]  # 'Bytes'
        elif len(parts) == 2:  # e.g., NatGateway-Bytes
            result['usage_category'] = 'NatGateway'
            result['instance_type_or_detail'] = parts[1]  # 'Bytes'
        return result

    if len(parts) == 1:
        # If there is no region (e.g., "CPUCredits:t3" or "EBSOptimized:c5.xlarge")
        usage_category_parts = parts[0].split(":")
        result['usage_category'] = usage_category_parts[0]  # "CPUCredits" or "EBSOptimized"
        
        if len(usage_category_parts) > 1:
            result['instance_type_or_detail'] = usage_category_parts[1]  # "t3" or "c5.xlarge"
    
    elif len(parts) >= 2:
        # For normal cases with region (e.g., "USW2-BoxUsage:m5.large")
        result['region'] = parts[0]
        result['usage_category'] = parts[1].split(":")[0]
        
        if ":" in parts[1]:
            result['instance_type_or_detail'] = parts[1].split(":")[1]
        
        # Handle special cases like DataTransfer
        if 'DataTransfer' in usage_type:
            result['additional_info'] = "-".join(parts[2:])
        elif len(parts) > 2:
            result['instance_type_or_detail'] = parts[2]
    
    return result


def filter_usage_type(df, pattern):
    return df[df['line_item_usage_type'].str.contains(pattern, na=False)]



def generate_json_data(input):
    final_json = {}
    accounts = input
    for i in accounts:
        if i["account_type"] =="cur":
            cost_column = "line_item_blended_cost"
            account_id_column = 'line_item_usage_account_id'
            # df = i["dataframe"]
            start = time.time()
            query = i["query"]
            df = wr.athena.read_sql_query(query, database="athena")
            end = time.time()
            print("time taken for aws_cur:",end-start)
        if i["account_type"] =="billing_group":
            cost_column = "line_item_unblended_cost"
            account_id_column = 'line_item_usage_account_id'    
            # df = i["dataframe"]
            query = i["query"]
            start = time.time()
            df = wr.athena.read_sql_query(query, database="athena")
            end = time.time()
            print("time taken for billling_group:",end-start)
    # account_type = ["cur","billing_group"]
    # for i in account_type:
    #     if i =="cur":
    #         cost_column = "line_item_blended_cost"
    #         account_id_column = 'bill_payer_account_id'
    #         df = pd.read_csv(r"c:\Users\user\Downloads\kpoint-test-account-from-cur.csv")
    #     if i =="billing_group":
    #         cost_column = "line_item_unblended_cost"
    #         account_id_column = 'line_item_usage_account_id'    
    #         df = pd.read_csv(r"701980022429.csv")

        chargeby_service = []
        savings = []
        json_data = {}
        Billing_details = {}
        bill_summary = []
        highest_service_details=[]
        # df = pd.read_csv(r"c:\Users\user\Downloads\53ffbaa7-12e8-4cc8-b0c3-e3cd7f02e61a.csv")
        df['is_flagged'] = False
        df['line_item_usage_start_date'] = pd.to_datetime(df['line_item_usage_start_date'])
        df['line_item_usage_end_date'] = pd.to_datetime(df['line_item_usage_end_date'])

        account_id = df[account_id_column].unique()
        account_id = int(account_id[0])
        mask = ~((df['line_item_usage_end_date'].dt.day == 1) & 
                (df['line_item_usage_end_date'].dt.hour == 0) & 
                (df['line_item_usage_end_date'].dt.minute == 0))
        # excluded_1st_end_month = df[~((df['line_item_usage_end_date'].dt.day == 1) & (df['line_item_usage_end_date'].dt.hour == 0) & (df['line_item_usage_end_date'].dt.minute == 0))]
        min_start_date = df['line_item_usage_start_date'].min()
        max_end_date = df.loc[mask, 'line_item_usage_end_date'].max()
        min_start_date_str = min_start_date.strftime('%b %d').lstrip('0')
        max_end_date_str = max_end_date.strftime('%b %d, %Y').lstrip('0')

        # Combine the two formatted date strings into the final output
        formatted_date_range = f"{min_start_date_str} - {max_end_date_str}"
        billing_date = formatted_date_range.split(" - ")[0] + ", " + formatted_date_range.split(", ")[1]
        billing_date =  datetime.strptime(billing_date, "%b %d, %Y")
        billing_date = billing_date.strftime("%Y-%m")

        print(billing_date)
        
        total_cost = df[cost_column].sum()
        Billing_details["Account_id"] = account_id

        Billing_details["billing_period"] = formatted_date_range
        Billing_details["total_cost"] = total_cost
        json_data["account_details"] = [Billing_details]
        # df['line_item_usage_amount'] = pd.to_numeric(df['line_item_usage_amount'], errors='coerce')
        # df[cost_column] = pd.to_numeric(df['line_item_usage_amount'], errors='coerce')
        print(Billing_details)
        aws_services = df['bill_billing_entity'].unique()

        print("total cost:", total_cost)

        usage_type = df["line_item_line_item_type"].unique()


        if "SavingsPlanCoveredUsage" in usage_type:
            relevant_columns = ["line_item_line_item_type", "line_item_product_code","line_item_usage_amount", cost_column]
            SavingsPlanCoveredUsage =df[relevant_columns].query("line_item_line_item_type == 'SavingsPlanCoveredUsage'")
            # SavingsPlanCoveredUsage = SavingsPlanCoveredUsage
            # SavingsPlanCoveredUsage = df[df["line_item_line_item_type"]=='SavingsPlanCoveredUsage']
            aggregated_data = (
                SavingsPlanCoveredUsage.groupby('line_item_product_code')
                .agg({cost_column: 'sum','line_item_usage_amount': 'sum' })
                .reset_index())            
            # aggregated_data.to_csv("data\\aggreagated.csv")
            # SavingsPlanCoveredUsage = SavingsPlanCoveredUsage.groupby(['line_item_product_code']).agg({cost_column: 'sum','line_item_usage_amount': 'sum' }).reset_index()
            # SavingsPlanCoveredUsage["line_item_usage_amount_with_units"] = SavingsPlanCoveredUsage['line_item_usage_amount'].astype(str) + ' ' + SavingsPlanCoveredUsage['operation unit'].astype(str)
            service_total = aggregated_data[cost_column].sum()
            # service_total = SavingsPlanCoveredUsage[cost_column].sum()
            # print(SavingsPlanCoveredUsage)
            print(service_total)
            service_entry = {
                    'service_name': "Savings Plans Discounts",
                    'service_total':service_total,
                    'regions': [],
                    'Global' : []
                }    
            
            Global_entry = {"Global_total":service_total,"patterns" : []}
            
            # services = SavingsPlanCoveredUsage["line_item_product_code"].unique()
            # print(services)
            for service in aggregated_data['line_item_product_code'].unique():
            # for service in services:
                pattern_total = aggregated_data[aggregated_data["line_item_product_code"]==service][cost_column].sum()
                # pattern_data = aggregated_data[aggregated_data["line_item_product_code"]==service]
                # pattern_total = pattern_data[cost_column].sum()
                pattern_entry = {
                    "pattern" : service,
                    "pattern_total" : pattern_total,
                    "line_items":[]
                }
                Global_entry["patterns"].append(pattern_entry)
            
            service_entry["Global"].append(Global_entry)
            
            savings.append(service_entry)
            print(savings)

        # if 'SavingsPlanRecurringFee' in usage_type:
        #     SavingsPlanRecurringFee = df.loc[df["line_item_line_item_type"]=='SavingsPlanRecurringFee',["line_item_line_item_description", "product_location", "line_item_usage_amount", cost_column]].groupby(['line_item_line_item_description','product_location']).agg({cost_column: 'sum','line_item_usage_amount': 'sum' }).reset_index()
        #     # SavingsPlanRecurringFee = SavingsPlanRecurringFee.groupby(['line_item_line_item_description','product_location']).agg({cost_column: 'sum','line_item_usage_amount': 'sum' }).reset_index()
        #     service_total = SavingsPlanRecurringFee[cost_column].sum()
            
        #     # SavingsPlanRecurringFee.to_csv("SavingsPlanRecurringFee.csv")
        #     service_entry = {
        #         'service_name': "Savings Plans for AWS Compute Usage",
        #         'service_total':service_total,
        #         'regions': [],
        #         'Global' : []
        #     }
        #     # print(service_entry)
        #     region_entry = {
        #     'region': "Any",
        #     'region_total': service_total,
        #     'patterns': []
        #     }
        #     pattern_entry = {
        #     'pattern': "Compute Savings Plan",
        #     'pattern_total': service_total,
        #     'line_items':  SavingsPlanRecurringFee[["line_item_line_item_description", "line_item_usage_amount",cost_column]].to_dict(orient='records')
        # }
            
        #     region_entry["patterns"].append(pattern_entry)
        #     service_entry["regions"].append(region_entry)
        #     # print(service_entry)
        #     chargeby_service.append(service_entry)

        if "AWS Marketplace" in aws_services:
            marketplace_data = df.loc[df['bill_billing_entity'] == "AWS Marketplace",["line_item_line_item_description",'pricing_unit','line_item_usage_amount',cost_column]]
            marketplace_data_grouped = marketplace_data.groupby(["line_item_line_item_description",'pricing_unit']).agg({cost_column: 'sum','line_item_usage_amount': 'sum' }).reset_index()
            marketplace_data_grouped["line_item_usage_amount_with_units"] = marketplace_data_grouped['line_item_usage_amount'].astype(str) + ' ' + marketplace_data_grouped['pricing_unit'].astype(str)
            marketplace_total = marketplace_data[cost_column].sum()
            df.loc[marketplace_data.index, 'is_flagged'] = True
            service_entry = {
                    'service_name': "AWS Marketplace",
                    'service_total':marketplace_total,
                    'regions': [],
                    'Global' : []
                }    
            Global_entry = {"Global_total":marketplace_total,"patterns": [],
                            "line_items":(marketplace_data_grouped[["line_item_line_item_description", "line_item_usage_amount_with_units",cost_column]]).to_dict(orient='records')}
            service_entry['Global'].append(Global_entry)
            summary_entry = {"service_provider":"Amazon Web Services,Inc. - Marketplace","service_total":marketplace_total}
            
            bill_summary.append(summary_entry)
            chargeby_service.append(service_entry)
            
        if i["account_type"] =="cur":
            aws_services = df["product_servicecode"][(df["is_flagged"] == False) & (df["product_servicecode"].notna()) & (df["product_servicecode"] != '')].unique()
            service_provide_total = df[cost_column][(df["is_flagged"] == False) & (df["product_servicecode"].notna()) & (df["product_servicecode"] != '')].sum()              
        if i["account_type"] =="billing_group":        
            aws_services = df["product_servicecode"][df["is_flagged"] == False].unique()
            service_provide_total = df[cost_column][(df["is_flagged"] == False)].sum()
        print("no of services used:", len(aws_services),aws_services)
        
    #     # aws_services=["AmazonEC2"]
        summary_entry = {"service_provider":"Amazon Web Services India Private Limited","service_total":service_provide_total}
        bill_summary.append(summary_entry)
        for service in aws_services:
            servicedata = df[(df["product_servicecode"] == service) & (df["is_flagged"] == False)]
            service_total = servicedata[cost_column].sum()
            print(service,service_total)
            # region_null = (servicedata[['product_location']].isnull().sum()).sum()
            region_null = (servicedata['product_location'] == '').sum() + (servicedata['product_location'].str.isspace()).sum()
            
            # print(region_null)
            service_entry = {
                    'service_name': service,
                    'service_total':service_total,
                    'regions': [],
                    'Global' : []
                }

        
                
            if service == "AmazonEC2":
                ece_result = []
                # patterns = ["EBS", "SpotUsage", "BoxUsage", "Nat", "CPUCredits", "ElasticIP"]
                servicedata.loc[servicedata["line_item_line_item_type"] == "SavingsPlanNegation", "line_item_line_item_description"] = (
                    servicedata["product_instance_type"] + " " +
                    servicedata["product_product_family"] + " "+
                    " Covered by compute savings Plan"
                )
                line_items = servicedata["line_item_usage_type"].unique()
                patterns = set()
                parsed_data = {}

        # Apply the function to each usage type and store results
                for usage in line_items:
                    parsed = parse_usage_type(usage)
                    parsed_data[usage] = parsed
                    if parsed["usage_category"]:
                        patterns.add(parsed["usage_category"])

                if 'EBSOptimized' in patterns:
                    patterns.remove('EBSOptimized')    
                for pattern in patterns:
                    filtered_data = filter_usage_type(servicedata, pattern)
                    filtered_data["pattern"] = pattern
                    grouped_df = filtered_data.groupby([
                        'product_location',
                        'pattern',
                        'line_item_usage_type',
                        'line_item_line_item_description',
                        'pricing_unit'
                    ]).agg({cost_column: 'sum','line_item_usage_amount': 'sum'}).reset_index()
                    ece_result.append(grouped_df)
                
                final_grouped_df = pd.concat(ece_result, ignore_index=True)
                final_grouped_df = final_grouped_df.sort_values(by=['product_location', 'pattern'])
                final_grouped_df["line_item_usage_amount_with_units"] = final_grouped_df['line_item_usage_amount'].astype(str) + ' ' + final_grouped_df['pricing_unit'].astype(str)
                final_grouped_df["line_item_usage_type_customized"] = "AWS" + " "  + service + " " + final_grouped_df['line_item_usage_type'].astype(str) 
                for region in final_grouped_df['product_location'].unique():
                    region_data = final_grouped_df[final_grouped_df['product_location'] == region]
                    region_total = region_data[cost_column].sum()
                    region_entry = {
                        'region': region,
                        'region_total': region_total,
                        'patterns': []
                    }

                    patterns_in_region = region_data['pattern'].unique()
                    for pattern in patterns_in_region:
                        pattern_data = region_data[region_data['pattern'] == pattern]
                        pattern_total = pattern_data[cost_column].sum()
                        
                        pattern_entry = {
                            'pattern': pattern,
                            'pattern_total': pattern_total,
                            'line_items':  pattern_data[["line_item_line_item_description","line_item_usage_type_customized","line_item_usage_amount_with_units", cost_column]].to_dict(orient='records')
                        }
                        
                        region_entry['patterns'].append(pattern_entry)
                        
                    service_entry["regions"].append(region_entry)
                chargeby_service.append(service_entry)
                
            elif service == "AWSELB":
                sevice_name = "Elastic Load Balancing"
                grouped_df = servicedata.groupby([
                    'line_item_line_item_description',
                    'product_location',  
                    'line_item_usage_type', 
                    'product_product_family',
                    'line_item_line_item_type',
                    'pricing_unit'
                ]).agg({
                    'line_item_usage_amount': 'sum' ,
                    cost_column: 'sum'
                    # Aggregate by summing usage amount
                }).reset_index()
                grouped_df["line_item_usage_amount_with_units"] = grouped_df['line_item_usage_amount'].astype(str) + ' ' + grouped_df['pricing_unit'].astype(str)
                grouped_df["line_item_usage_type_customized"] = sevice_name + "-" + grouped_df['product_product_family'].str.split('-').str[1]
                sorted_df = grouped_df.sort_values(by=['product_location', 'line_item_usage_amount'], ascending=[True, False])
            
                for region in sorted_df['product_location'].unique():
                        region_data = sorted_df[sorted_df['product_location'] == region]
                        region_total = region_data[cost_column].sum()
                        # print(region,region_total)
                        region_entry = {
                        'region': region,
                        'region_total': region_total,
                        'patterns': [],
                        'line_items':(region_data[["line_item_line_item_description","line_item_usage_type_customized" ,cost_column, "line_item_usage_amount_with_units"]].to_dict(orient='records'))}
                        # print(region_total)
                        service_entry["regions"].append(region_entry)
                chargeby_service.append(service_entry) 
                        
            elif service == "AWSCodeCommit" or service == "AmazonIVS" or service == "AmazonRoute53" and region_null>0:
                    # print("elif")
                    grouped_df = servicedata.groupby([
                    'line_item_line_item_description',  
                    'line_item_usage_type', 
                    'line_item_line_item_type',
                    'pricing_unit'
                ]).agg({
                    'line_item_usage_amount': 'sum' ,
                    cost_column: 'sum',

                    # Aggregate by summing usage amount
                }).reset_index()
                    grouped_df["line_item_usage_amount_with_units"] = grouped_df['line_item_usage_amount'].astype(str) + ' ' + grouped_df['pricing_unit'].astype(str)
                    grouped_df["line_item_usage_type_customized"] = "AWS" + " "  + service + " " + grouped_df['line_item_usage_type'].astype(str) 
                    Global_total = grouped_df[cost_column].sum()
                    # prfrom_location_or_to_locationint(servicedata[['product_location',  'product_from_region_code', 'line_item_usage_type', 'line_item_line_item_type']].isnull().sum())
                    # service_cost={service: grouped_df[cost_column].sum()}
                    # # total_sum_check.append(service_cost)
                    
                    # grouped_df["service"]=service
                    Global_entry = {"Global_total":Global_total,"patterns": [],
                            "line_items":(grouped_df[["line_item_line_item_description","line_item_usage_type_customized", "line_item_usage_amount_with_units",cost_column]].to_dict(orient='records'))}
                    service_entry['Global'].append(Global_entry)
                    chargeby_service.append(service_entry)
                    
            elif service =="AWSDataTransfer" and  region_null >0 :
                print("in aws datatransfer")
                servicedata['from_location_or_to_location'] = np.where(
                    servicedata['product_from_location'] != 'External', 
                    servicedata['product_from_location'], 
                    servicedata['product_to_location']
                )
                servicedata['type'] = np.where(servicedata['line_item_usage_type'].str.contains("DataTransfer", na=False), "bandwidth", "Data Transfer")
                types =  servicedata['type'].unique()
                grouped_df = servicedata.groupby([
                    'line_item_line_item_description',  
                    'line_item_usage_type', 
                    'type',
                    'line_item_line_item_type',
                    'from_location_or_to_location',
                    'pricing_unit'
                ]).agg({
                    'line_item_usage_amount': 'sum' ,
                    cost_column: 'sum'
                    # Aggregate by summing usage amount
                }).reset_index()
                grouped_df["line_item_usage_amount_with_units"] = grouped_df['line_item_usage_amount'].astype(str) + ' ' + grouped_df['pricing_unit'].astype(str)
                grouped_df["line_item_usage_type_customized"] =  service + " " + grouped_df['line_item_usage_type'].astype(str) 
                sorted_df = grouped_df.sort_values(by=['from_location_or_to_location','type' ,'line_item_usage_amount'], ascending=[True,False, False])
                # sorted_df.to_csv("data-transfer.csv")
                # service_cost={service: sorted_df[cost_column].sum()}
                
                # sorted_df["service"]=service
                
                # # total_sum_check.append(service_cost)
                
                # final_df.append(sorted_df)
                
                for region in sorted_df['from_location_or_to_location'].unique():
                        

                    region_data = sorted_df[sorted_df['from_location_or_to_location'] == region]
                    # region_data.to_csv(f"{region}.csv")
                    region_total = region_data[cost_column].sum()
                    
                    region_entry = {
                    'region': region,
                    'region_total': region_total,
                    'patterns': []
                    }
                    for type in types:
                        type_data = region_data[region_data["type"]==type]
                        type_sum = type_data[cost_column].sum()
                        pattern_entry = {
                            'pattern': type,
                            'pattern_total': type_sum,
                            'line_items':  type_data[["line_item_line_item_description","line_item_usage_type_customized","line_item_usage_amount_with_units" ,cost_column]].to_dict(orient='records')
                        }
                        region_entry["patterns"].append(pattern_entry)
                    # 'line_items':(region_data[["line_item_line_item_description", cost_column, "line_item_usage_amount"]].to_dict(orient='records'))}
                    # print(region_total)
                    service_entry["regions"].append(region_entry)
                chargeby_service.append(service_entry)    
                
            elif service == "AmazonCloudFront":
                servicedata['location_status'] = np.where(
                (servicedata['product_from_location'].isna() | (servicedata['product_from_location'] == '')) &
                (servicedata['product_to_location'].isna() | (servicedata['product_to_location'] == '')) &
                (servicedata['product_location'].isna() | (servicedata['product_location'] == '')),
                'global',  # Case 1: All fields are empty, set as 'global'

                np.where(
                    (~servicedata['product_from_location'].isna()) & (servicedata['product_from_location'] != '') &
                    (~servicedata['product_to_location'].isna()) & (servicedata['product_to_location'] != '') &
                    (servicedata['product_location'].isna() | (servicedata['product_location'] == '')),
                    servicedata['product_from_location'],  # Case 2: 'from' and 'to' are not empty, but 'location' is, keep 'from_location'

                    np.where(
                        (servicedata['product_from_location'].isna() | (servicedata['product_from_location'] == '')) &
                        (servicedata['product_to_location'].isna() | (servicedata['product_to_location'] == '')) &
                        (~servicedata['product_location'].isna()) & (servicedata['product_location'] != ''),
                        servicedata['product_location'],  # Case 3: 'location' is not empty, keep 'product_location'

                        np.nan  # Default case (optional, in case none of the conditions are met)
                    )))

                final = servicedata.groupby(["product_product_family","location_status","line_item_usage_type","pricing_unit","line_item_line_item_description"]).agg({cost_column: 'sum','line_item_usage_amount': 'sum'}).reset_index()
                final = final.sort_values(by=['location_status', 'product_product_family'])
                final["line_item_usage_amount_with_units"] = final['line_item_usage_amount'].astype(str) + ' ' + final['pricing_unit'].astype(str)
                final["line_item_usage_type_customized"] = "AWS" + " "  + "" + " " + final['line_item_usage_type'].astype(str) 




                for region in final['location_status'].unique():
                    region_data = final[final['location_status'] == region]
                    region_total = region_data[cost_column].sum()
                    region_entry = {
                        'region': region,
                        'region_total': region_total,
                        'patterns': []
                    }

                    patterns_in_region = region_data['product_product_family'].unique()
                    for pattern in patterns_in_region:
                        # if pattern =="Data Transfer":
                        #     pattern = "Bandwidth"
                        pattern_data = region_data[region_data['product_product_family'] == pattern]
                        pattern_total = pattern_data[cost_column].sum()
                        
                        pattern_entry = {
                            'pattern': pattern,
                            'pattern_total': pattern_total,
                            'line_items':  pattern_data[["line_item_line_item_description","line_item_usage_type_customized","line_item_usage_amount_with_units", cost_column]].to_dict(orient='records')
                        }
                        
                        region_entry['patterns'].append(pattern_entry)
                        
                    service_entry["regions"].append(region_entry)
                chargeby_service.append(service_entry)      
            elif region_null == 0 :
                print("elif null ==0")
                grouped_df = servicedata.groupby([
                    'line_item_line_item_description',
                    'product_location',  
                    'line_item_usage_type', 
                    'line_item_line_item_type',
                    'pricing_unit'
                ]).agg({
                    'line_item_usage_amount': 'sum' ,
                    cost_column: 'sum'
                    # Aggregate by summing usage amount
                }).reset_index()
                grouped_df["line_item_usage_amount_with_units"] = grouped_df['line_item_usage_amount'].astype(str) + ' ' + grouped_df['pricing_unit'].astype(str)
                grouped_df["line_item_usage_type_customized"] = "AWS" + " "  + service + " " + grouped_df['line_item_usage_type'].astype(str) 
                sorted_df = grouped_df.sort_values(by=['product_location', 'line_item_usage_amount'], ascending=[True, False])
                
                for region in sorted_df['product_location'].unique():
                        region_data = sorted_df[sorted_df['product_location'] == region]
                        region_total = region_data[cost_column].sum()
                        # print(region,region_total)
                        region_entry = {
                        'region': region,
                        'region_total': region_total,
                        'patterns': [],
                        'line_items':(region_data[["line_item_line_item_description","line_item_usage_type_customized" ,cost_column, "line_item_usage_amount_with_units"]].to_dict(orient='records'))}
                        # print(region_total)
                        service_entry["regions"].append(region_entry)
                chargeby_service.append(service_entry) 
                # service_cost={service: sorted_df[cost_column].sum()}
                # sorted_df["service"]=service
                
                # total_sum_check.append(service_cost)
                
                # final_df.append(sorted_df)
                # print(grouped_df[cost_column].sum())
            # print(len(grouped_df))
            
            
            elif region_null > 0:
                grouped_df = servicedata.groupby([
                    'line_item_line_item_description',  
                    'line_item_usage_type', 
                    'line_item_line_item_type',
                    'pricing_unit'
                ]).agg({
                    'line_item_usage_amount': 'sum' ,
                    cost_column: 'sum'
                    # Aggregate by summing usage amount
                }).reset_index()
                # print(grouped_df[cost_column].sum())
                grouped_df["line_item_usage_amount_with_units"] = grouped_df['line_item_usage_amount'].astype(str) + ' ' + grouped_df['pricing_unit'].astype(str)
                grouped_df["line_item_usage_type_customized"] = "AWS" + " "  + service + " " + grouped_df['line_item_usage_type'].astype(str) 
                sorted_df = grouped_df.sort_values(by=['line_item_line_item_description', 'line_item_usage_amount'], ascending=[True, False])
                # service_cost={service: sorted_df[cost_column].sum()}
                
                # sorted_df["service"]=service
                Global_total = grouped_df[cost_column].sum()
                    # prfrom_location_or_to_locationint(servicedata[['product_location',  'product_from_region_code', 'line_item_usage_type', 'line_item_line_item_type']].isnull().sum())
                    # service_cost={service: grouped_df[cost_column].sum()}
                    # # total_sum_check.append(service_cost)
                    
                    # grouped_df["service"]=service
                Global_entry = {"Global_total":Global_total,"patterns": [],
                        "line_items":(sorted_df[["line_item_line_item_description","line_item_usage_type_customized" ,cost_column, "line_item_usage_amount_with_units"]].to_dict(orient='records'))}
                service_entry['Global'].append(Global_entry)
                chargeby_service.append(service_entry)
                # total_sum_check.append(service_cost)
                
                # final_df.append(sorted_df)
            df.loc[servicedata.index, 'is_flagged'] = True
                    # final_df.append(grouped_df)
        # Convert the final output to JSON

        json_data["Charges by service"] = chargeby_service

        json_data["Savings"] = savings
        json_data["AWS estimated bill summary"] = bill_summary
        final_json[i["account_type"]]=json_data
        highest_service, highest_region = find_highest_spends(final_json)
        highest_service_entry = {"service_provider":"Amazon Web Services India Private Limited","service_name":highest_service["service_name"],"service_total":highest_service["service_total"],"region_name":highest_region["region"],"region_total":highest_region["region_total"]}
        highest_service_details.append(highest_service_entry)
        json_data["Highest estimated cost by service provider"] = highest_service_details
    # Calculate the sum of False values
        if i["account_type"] == "billing_group":
            false_count = (df['is_flagged'] == False).sum()
            if false_count > 0 :
                print("some services has been not processed yet")
            print(false_count)
    
    output_json = json.dumps(final_json, indent=4)

    with open(f"{account_id}_{billing_date}.json", "w") as json_file:
        json_file.write(output_json)

    # jsonTOpdf(output_json,billing_date)
        # Optionally, save the JSON to a file
        
    
    del df
    gc.collect()
# Print the JSON output
# print(output_json)
