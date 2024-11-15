import json
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.units import inch
from datetime import datetime
current_date = datetime.now().strftime("%b %d, %Y")

# FONT_BOLD_PATH = 'D://New folder//Amazon_Typefaces_Complete_Font_Set_Mar2020//Ember//AmazonEmber_Bd.ttf'
# FONT_REGULAR_PATH = 'D://New folder//Amazon_Typefaces_Complete_Font_Set_Mar2020//Ember//AmazonEmber_Rg.ttf'
# JSON_FILE_PATH = 'D://Json-pdf//701980022429_2024-10.json'
# PDF_OUTPUT_PATH = 'D://Json-pdf//Output files//Billing-oct-30.pdf'

pdfmetrics.registerFont(TTFont('AmazonEmberBold', 'AmazonEmber_Bd.ttf'))
pdfmetrics.registerFont(TTFont('AmazonEmberRegular', 'AmazonEmber_Rg.ttf'))

# def load_json_data(file_path):
#     try:
#         with open(file_path) as file:
#             return json.load(file)
#     except FileNotFoundError:
#         print(f"Error: File not found at {file_path}.")
#         return None
#     except json.JSONDecodeError:
#         print("Error: Failed to decode JSON.")
#         return None

'''def diff(json_data):
    services_cur = {ser['service_name']: ser['service_total'] for ser in json_data['cur']['Charges by service']}
    services_billing = {ser['service_name']: ser['service_total'] for ser in json_data['billing_group']['Charges by service']}

    differences_output = []

    for curname, cur_cost in services_cur.items():
        billing = services_billing.get(curname)
        if billing is None:
            continue
        difference = abs(cur_cost - billing)
        percentage = (difference / cur_cost) * 100 if cur_cost != 0 else 0
        differences_output.append((curname, cur_cost, billing, difference, percentage))

    cur_total_cost = json_data['cur']['account_details'][0]['total_cost']
    billing_total_cost = json_data['billing_group']['account_details'][0]['total_cost']
    total_cost_difference = abs(cur_total_cost - billing_total_cost)
    total_cost_percentage = (total_cost_difference / cur_total_cost) * 100 if cur_total_cost != 0 else 0

    differences_output.append(("Total", cur_total_cost, billing_total_cost, total_cost_difference, total_cost_percentage))

    return differences_output

diff(json_data)'''

def find_highest_spends(data):
    services = data["billing_group"]["Charges_by_service"]["aws"]
    highest_service = max(services, key=lambda x: x['service_total'])

    regions = []
    for service in services:
        regions.extend(service.get('regions', [])) 
    highest_region = max(regions, key=lambda x: x['region_total'])

    return highest_service, highest_region

def jsonTOpdf(json_data, billing_date):
    # try:
    #     with open(JSON_FILE_PATH,'r') as file:
    #         data = json.load(file)
    # except FileNotFoundError:
    #     print(f"Error: File not found at {JSON_FILE_PATH}.")
    #     return None
    # except json.JSONDecodeError:
    #     print("Error: Failed to decode JSON.")
    #     return None

    data = json.loads(json_data)
    account_details = data["billing_group"].get('account_details', [])
    if account_details:
        account_id = str(account_details.get("Account_id", ""))
        billing_period = account_details.get("billing_period", "")
    else:
        account_id = "N/A"
        billing_period = "N/A"
    date_printed = current_date
    pdf_fp =f"{account_id}_{billing_date}.pdf"
    c = canvas.Canvas(pdf_fp, pagesize=letter)
    width, height = letter
    c.setFont("AmazonEmberRegular", 8)

    total_cost = sum(service['service_total'] for service in data["billing_group"]["Charges_by_service"]["aws"])
    
    
    if 'marketplace' in data["billing_group"]["Charges_by_service"]:
        total_cost = total_cost + sum(service['service_total'] for service in data["billing_group"]["Charges_by_service"]["marketplace"])
    #print(total_cost)
    highest_service, highest_region = find_highest_spends(data)

    def find_highest_market():
      if 'marketplace' in data["billing_group"]["Charges_by_service"]:  
        services_market = data["billing_group"]["Charges_by_service"]["marketplace"]
        highest_service_market = max(services_market, key=lambda x: x['service_total'])

        return highest_service_market
    find_highest_market()
    
    def add_summary_page():
        c.setFont("AmazonEmberBold", 18)
        title = "AWS Estimated Bill Summary"
        image_path = "white2.jpg"
        image_width = 1 * inch
        image_height = 0.8 * inch
        c.drawImage(image_path, 30, height - image_height - 30, width=image_width, height=image_height)
        c.drawString(30, height - 145, title)

        right_margin = width - 30

        c.setFont("AmazonEmberBold", 12)
        account_label = "Account ID"
        c.setFillColorRGB(0.5, 0.5, 0.5)
        c.drawString(right_margin - c.stringWidth(account_label) - 135, height - 62, account_label)
        c.setFillColorRGB(0, 0, 0)

        c.setFont("AmazonEmberRegular", 10)
        c.drawString(right_margin - c.stringWidth(account_id) - 135, height - 77, account_id)

        c.setFont("AmazonEmberBold", 12)
        c.setFillColorRGB(0.5, 0.5, 0.5)
        c.drawString(right_margin - c.stringWidth("Billing Period") - 230, height - 62, "Billing Period")
        c.setFillColorRGB(0, 0, 0)

        c.setFont("AmazonEmberRegular", 10)
        c.drawString(right_margin - c.stringWidth(billing_period) - 230, height - 77, billing_period)

        c.setFont("AmazonEmberBold", 12)
        c.setFillColorRGB(0.5, 0.5, 0.5)
        c.drawString(right_margin - c.stringWidth("Date of Print") - 30, height - 62, "Date of Print")
        c.setFillColorRGB(0, 0, 0)
        c.setFont("AmazonEmberRegular", 10)
        c.drawString(right_margin - c.stringWidth(date_printed) - 30, height - 77, date_printed)

        c.setStrokeColorRGB(0.85, 0.85, 0.85)
        c.setLineWidth(0.7)
        c.line(right_margin - 220, height - 45, right_margin - 220, height - 80)
        c.line(right_margin - 120, height - 45, right_margin - 120, height - 80)

        c.setFont("AmazonEmberRegular", 10)
        c.setFillColorRGB(0.95, 0.95, 0.95)
        estimated_total_str = "Estimated Grand Total:"
        c.setFillColorRGB(0, 0, 0)
        c.drawString(40, height - 175, estimated_total_str)

        c.setFont("AmazonEmberBold", 18)
        total_cost_str = f"USD {total_cost:,.2f}"
        total_cost_x = width - c.stringWidth(total_cost_str) - 30
        c.drawString(total_cost_x, height - 175, total_cost_str)

        
        c.setFillColorRGB(0.97, 0.97, 0.97)
        c.rect(30, height - 230, width - 40, 12, stroke=0, fill=1)
        c.setFillColorRGB(0, 0, 0)
        c.setFont("AmazonEmberBold", 10)
        c.drawString(30, height - 210, "Amazon Web Services India Private Limited")
        c.setFont("AmazonEmberBold", 8)
        c.drawString(30, height - 228, "Highest service spend")
        c.setFont("AmazonEmberRegular", 10)
        c.drawString(30, height - 245, "Service name")
        c.drawString(total_cost_x, height - 245, highest_service['service_name'])
        c.drawString(30, height - 268, "Highest service spend")
        c.drawString(total_cost_x, height - 265, f"USD {highest_service['service_total']:,.2f}")

        c.setFont("AmazonEmberBold", 8)
        c.setFillColorRGB(0.97, 0.97, 0.97)
        c.rect(30, height - 330, width - 40, 12, stroke=0, fill=1)
        c.setFillColorRGB(0, 0, 0)
        c.drawString(30, height - 328, "Highest AWS Region spend")
        c.setFont("AmazonEmberRegular", 10)
        c.drawString(30, height - 345, "Region name")
        c.drawString(total_cost_x, height - 345, highest_region['region'])
        c.drawString(30, height - 365, "Highest AWS Region spend")
        c.drawString(total_cost_x, height - 365, f"USD {highest_region['region_total']:,.2f}")

        highest_service_market = find_highest_market()
        if highest_service_market:
            c.setFillColorRGB(0.97, 0.97, 0.97)
            c.rect(30, height - 435, width - 40, 12, stroke=0, fill=1)
            c.setFillColorRGB(0, 0, 0)
            c.setFont("AmazonEmberBold", 10)
            c.drawString(30, height - 417, "Amazon Web Services, Inc. - Marketplace")
            c.setFont("AmazonEmberBold", 8)
            c.drawString(30, height - 430, "Highest service spend")
            c.setFont("AmazonEmberRegular", 10)
            c.drawString(30, height - 460, "Service name")
            c.drawString(total_cost_x-65, height - 460, highest_service_market['service_name'])
            c.drawString(30, height - 480, "Highest service spend")
            c.drawString(total_cost_x, height - 480, f"USD {highest_service_market['service_total']:,.2f}")

        c.showPage()

    add_summary_page()
    y_position = height - 40
    description_col_width = 200
    usage_col_width = 150
    #amount_col_width = 100
    usage_amount_header_x = 30 + description_col_width + (usage_col_width - 20)

    def draw_headers(y_pos):
        c.setFillColorRGB(0.97, 0.97, 0.97)
        c.rect(30, y_pos - 6.5, width - 60, 18, stroke=0, fill=1)
        c.setFillColorRGB(0, 0, 0)
        c.setFont("AmazonEmberBold", 9)
        c.drawString(30, y_pos, "Description")
        c.drawString(usage_amount_header_x, y_pos, "Usage Amount")
        c.drawString(width - c.stringWidth("Amount in USD") - 30, y_pos, "Amount in USD")
        y_pos -= 6
        c.setStrokeColorRGB(0.85, 0.85, 0.85)
        c.setLineWidth(0.7)
        c.line(30, y_pos, width - 30, y_pos)
        return y_pos - 10
    
    def check_page_space(y_pos):
        if y_pos < 50:
            c.showPage()
            y_pos = height - 50
            y_pos = draw_headers(y_pos)
        return y_pos
    
    c.setFont("AmazonEmberBold", 16)
    y_position -= 15
    c.drawString(30, y_position, "Charges by Service")
    y_position -= 20
    y_position = draw_headers(y_position)

    if 'marketplace' in data["billing_group"]["Charges_by_service"]:
        c.setFont("AmazonEmberBold",10)
        y_position -= 7
        c.drawString(30, y_position, "Amazon Web Services, Inc. - Marketplace")
        y_position -= 15

    def draw_charge_entry(c, y_position, width, entry_name, entry_total, font="AmazonEmberBold", font_size=8, color=(0, 0, 0), draw_line=False, line_width=0.5, x_offset=30):
        c.setFont(font, font_size)
        c.setFillColorRGB(*color) 
        c.drawString(x_offset, y_position, entry_name)
        entry_total_str = f"USD {entry_total:.2f}"
        c.drawString(width - c.stringWidth(entry_total_str) - 30, y_position, entry_total_str)
        if draw_line:
            c.setStrokeColorRGB(0.85, 0.85, 0.85)
            c.setLineWidth(line_width)
            c.line(30, y_position - 3, width - 30, y_position - 3)
        return y_position - 10 if draw_line else y_position - 8

    def draw_line_item(c, y_position, width, line_item, usage_amount_header_x, is_pattern_header=False):
        if is_pattern_header:
            pattern_name = f"{line_item['pattern']}:"
            amount_str = f"USD ({line_item['pattern_total']:.2f})"
            
            c.setFont("AmazonEmberRegular", 8)
            c.drawString(30, y_position, pattern_name)
            c.drawString(width - c.stringWidth(amount_str) - 30, y_position, amount_str)
            return y_position - 10
        else:
            description = line_item.get('line_item_line_item_description', '')
            usage_value_str = f"{float(str(line_item['line_item_usage_amount_with_units']).split(' ')[0]):.2f} {str(line_item['line_item_usage_amount_with_units']).split(' ')[1]}"
            cost_amount_str = f"USD {line_item['line_item_unblended_cost']:.2f}"
        
            c.setFont("AmazonEmberRegular", 6.5)
            c.setFillColorRGB(0.5, 0.5, 0.5)
            c.drawString(48, y_position, description)
            c.setFont("AmazonEmberRegular", 7)
            c.drawString(usage_amount_header_x, y_position, usage_value_str)
            c.drawString(width - c.stringWidth(cost_amount_str) - 30, y_position, cost_amount_str)
            c.setFillColorRGB(0, 0, 0)
            return y_position - 10

    for service in data["billing_group"]["Charges_by_service"]:
        if 'marketplace' in service:
            market_data = data["billing_group"]["Charges_by_service"]['marketplace']
            
            for charge in market_data:
                y_position = check_page_space(y_position)
                y_position = draw_charge_entry(c, y_position, width, charge['service_name'], charge['service_total'], draw_line=True)

                if 'Global' in charge:
                    for global_item in charge['Global']:
                        for item in global_item['line_items']:
                            y_position = draw_line_item(c, y_position, width, item, usage_amount_header_x)
                            
                if 'regions' in charge:
                    for region in charge['regions']:
                        y_position = check_page_space(y_position)
                        y_position = draw_charge_entry(c, y_position, width, region['region'], region.get('region_total', 0), draw_line=False)

                        if 'line_items' in region:
                            for item in region['line_items']:
                                y_position = draw_line_item(c, y_position, width, item, usage_amount_header_x)

                        if 'patterns' in region:
                            for pattern in region['patterns']:
                                y_position = draw_charge_entry(c, y_position, width, pattern['pattern'], pattern['pattern_total'], draw_line=False, x_offset=45)
                                for line_item in pattern['line_items']:
                                    y_position = draw_line_item(c, y_position, width, line_item, usage_amount_header_x)


                            y_position -= 7
                    y_position -= 7

                    
    sorted_services = sorted(data["billing_group"]['Charges_by_service']['aws'], key=lambda x: x['service_total'], reverse=True)
    c.setFont("AmazonEmberBold", 10)
    y_position -= 15
    c.drawString(30, y_position, "Amazon Web Services India Private Limited")
    y_position -= 15

    for service in sorted_services:
        y_position = check_page_space(y_position)
        y_position = draw_charge_entry(c, y_position, width, service['service_name'], service['service_total'], font="AmazonEmberBold", font_size=9, draw_line=True)
        if 'Global' in service:
            for global_item in service['Global']:
                if "line_items" in global_item:
                    for item in global_item['line_items']:
                        if 'line_item_usage_type_customized' in item:
                            
                            y_position = draw_charge_entry(
                                c, y_position, width, str(item['line_item_usage_type_customized']), 
                                item['line_item_unblended_cost'], font="AmazonEmberRegular", font_size=7
                            )
                            y_position = draw_line_item(c, y_position, width, item, usage_amount_header_x)
                        else:
                           
                            y_position = draw_line_item(c, y_position, width, item, usage_amount_header_x)

                      
                        if y_position < 40:
                            c.showPage()
                            c.setFont("AmazonEmberRegular", 9)
                            y_position = height - 50
                            y_position = draw_headers(y_position)

                if 'patterns' in global_item:
                    for pattern in global_item['patterns']:
                        y_position = draw_charge_entry(
                            c, y_position, width, f"{pattern['pattern']}:", pattern['pattern_total'], font="AmazonEmberBold", font_size=8)

        if 'regions' in service and service['regions']:
            for region in service['regions']:
                y_position = check_page_space(y_position)
                y_position = draw_charge_entry(c, y_position, width, region['region'], region.get('region_total', 0), font="AmazonEmberBold", font_size=8)

                if 'line_items' in region:
                    for item in region['line_items']:
                        
                        y_position = draw_charge_entry(c, y_position, width, str(item['line_item_usage_type_customized']), item['line_item_unblended_cost'], font="AmazonEmberRegular", font_size=7)
                        y_position = draw_line_item(c, y_position, width, item, usage_amount_header_x)
                        
                        if y_position < 40:
                            c.showPage()
                            c.setFont("AmazonEmberRegular", 8)
                            y_position = height - 50

                if 'patterns' in region:
                    for pattern in region['patterns']:
                        y_position = draw_charge_entry(
                            c, y_position, width, f"{pattern['pattern']}:", pattern['pattern_total'], font="AmazonEmberBold", font_size=8
                        )

                        for line_item in pattern['line_items']:
                            y_position = draw_charge_entry(
                                c, y_position, width, str(line_item['line_item_usage_type_customized']), line_item['line_item_unblended_cost'], font="AmazonEmberRegular", font_size=7
                            )
                            y_position = draw_line_item(c, y_position, width, line_item, usage_amount_header_x)

                            if y_position < 40:
                                c.showPage()
                                c.setFont("AmazonEmberRegular", 8)
                                y_position = height - 50
                                y_position = draw_headers(y_position)

        y_position -= 10

    if'Savings' in data["billing_group"]:
        c.setFont("AmazonEmberBold", 12)
        c.drawString(30, y_position, "Savings")
        y_position -= 15
        savings_data = data["billing_group"]['Savings']
        if savings_data:
            for saving in savings_data:
                c.setFont("AmazonEmberBold", 8)
                saving_description = saving['service_name']
                saving_amount = f"USD {saving['service_total']:.2f}"
                c.drawString(30, y_position, saving_description)
                c.drawString(width - c.stringWidth(saving_amount) - 30, y_position, saving_amount)
                y_position -= 15
                c.setStrokeColorRGB(0.85, 0.85, 0.85)
                c.setLineWidth(0.5)
                c.line(30, y_position, width - 30, y_position)
                y_position -= 10
                if 'Global' in saving:
                    for global_item in saving['Global']:
                        if 'patterns' in global_item:
                            for pattern in global_item['patterns']:
                                y_position = draw_line_item(c, y_position, width, pattern, usage_amount_header_x, is_pattern_header=True)
                                for line_item in pattern['line_items']:
                                    y_position = draw_line_item(c, y_position, width, line_item, usage_amount_header_x)
                                    if y_position < 40:
                                        c.showPage()
                                        c.setFont("AmazonEmberRegular", 8)
                                        y_position = check_page_space(y_position)

        c.setStrokeColorRGB(0.85, 0.85, 0.85)
        c.setLineWidth(0.5)
        c.line(30, y_position, width - 30, y_position)
        y_position -= 25



    """differences_output = (diff(json_data))
       
    c.setFont("AmazonEmberBold", 16)
    c.drawString(30, y_position, "Discounts by Cloudevolve")
    y_position -= 30

    c.setFillColorRGB(0.97, 0.97, 0.97)
    c.rect(30, y_position - 6.5, width - 60, 18, stroke=0, fill=1)
    c.setFillColorRGB(0, 0, 0)
    c.setFont("AmazonEmberBold", 8.5)
    c.drawString(30, y_position, "Service Name")
    c.drawString(180, y_position, "Billing Cost")
    c.drawString(260, y_position, "Master")
    c.drawString(365, y_position, "Differences")
    c.drawString(530, y_position, "Percentage")
    y_position -= 20
    c.setStrokeColorRGB(0.85, 0.85, 0.85)
    c.setLineWidth(0.7)

    c.setFont("AmazonEmberRegular", 6)

    def format_number(value):
        return f"{value:<10.2f}"

    for service_name, cur_cost, billing, difference, percentage in differences_output[:-1]: 
        c.drawString(30, y_position, service_name)
        c.drawString(180, y_position, format_number(billing)) 
        c.drawString(260, y_position, format_number(cur_cost))  
        c.drawString(365, y_position, format_number(difference))
        c.drawString(530, y_position, f"{percentage:.2f}%")
        
        y_position -= 15

        if y_position < 50:
            c.showPage()  
            y_position = 800

            c.setFont("AmazonEmberBold", 8.5)
            c.drawString(30, y_position, "Service Name")
            c.drawString(180, y_position, "Billing Cost")
            c.drawString(260, y_position, "Master")
            c.drawString(365, y_position, "Discount")
            c.drawString(530, y_position, "Percentage")
            y_position -= 20
            c.setFont("AmazonEmberRegular", 6)

    c.setStrokeColorRGB(0.85, 0.85, 0.85)
    c.setLineWidth(0.5) 
    c.line(30, y_position, width - 30, y_position) 
    y_position -= 15

    c.setFont("AmazonEmberBold", 10)
    total_cost_name, cur_total_cost, billing_total_cost, total_cost_difference, total_cost_percentage = differences_output[-1]
    c.drawString(30, y_position, total_cost_name) 
    c.drawString(180, y_position,format_number(billing_total_cost)) 
    c.drawString(180, y_position,format_number(cur_total_cost))
    c.drawString(365, y_position,format_number(total_cost_difference)) 
    c.drawString(530, y_position, f"{total_cost_percentage:.2f}%")  

    y_position -= 20  """

    c.save()

# def main():
    #try:
# jsonTOpdf(JSON_FILE_PATH,PDF_OUTPUT_PATH)
# print(f"PDF successfully generated at{p}.")
    #except Exception as e:
     #   print(f"Error generating PDF: {e}")

# if __name__ == "__main__":
#     main()