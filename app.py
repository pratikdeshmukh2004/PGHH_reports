import requests
from bs4 import BeautifulSoup
from pprint import pprint
from datetime import datetime, date, timedelta
import openpyxl
import concurrent.futures, csv
from mail import send_email
import traceback, os

today = date.today() - timedelta(days=1)
headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'accept-language': 'en-US,en;q=0.8',
    'cache-control': 'no-cache',
    'content-type': 'application/x-www-form-urlencoded',
    'origin': 'https://www.powerz.in',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Brave";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'sec-gpc': '1',
    'referer': 'https://www.powerz.in/powerz/kwhreports/parameter_view.php?mid=1001',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'
}
    

def get_current_week():
    week_number = today.isocalendar()[1]
    start_of_week = today - timedelta(days=today.weekday())  # Monday
    end_of_week = start_of_week + timedelta(days=6)          # Sunday
    start_of_week_str = start_of_week.strftime("%d/%m/%Y")
    end_of_week_str = end_of_week.strftime("%d/%m/%Y")
    result = f"Week #{week_number} [{start_of_week_str} - {end_of_week_str}]"
    return result

def get_UIDs():
    workbook = openpyxl.load_workbook('UID Data.xlsx')
    sheet = workbook.active
    rows = list(sheet.iter_rows(values_only=True))
    data = []
    columns = rows[0]
    for row in rows[1:]:
        row_data = {}
        for key, value in zip(columns, row):
            row_data[key] = value
        data.append(row_data) 
    workbook.close()
    return data


def get_operating_hours(uuid):
    uid = str(uuid['UID']).split("-")[-1].strip()
    print("Fetching time:", uid)
    selected_date = today.strftime("%Y-%m-%d")
    url = f"https://www.powerz.in/powerz/asms/currentreadingv2.php?selecteddate={selected_date}&db=pithampur&meter_primary_id={uid}"
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table')
    rows = table.find_all('tr')
    if len(rows)<2:
        return {"UID": uuid['UID'], "operating_hours": 1} 
    row = rows[-1].find_all("td")
    start_time = row[-2].text
    end_time = row[-1].text
    start_time_object = datetime.strptime(start_time, "%H:%M")
    end_time_object = datetime.strptime(end_time, "%H:%M")
    if end_time_object < start_time_object:
        end_time_object += timedelta(days=1)
    time_difference = end_time_object - start_time_object

    return {"UID": uuid['UID'], "operating_hours": time_difference.total_seconds() / 3600}

def get_kwh_fot_today_UID(uuid):
        uid = str(uuid['UID']).split("-")[-1].strip()
        print("Fetching:", uid)
        url = f"https://www.powerz.in/powerz/kwhreports/rptdaily.php?page=m&elw=1&mid={uid}&savedailytarget=1"
        currentweekval = get_current_week()
        payload = f'groupid=&meteridsel=1001&curweekval={currentweekval}&showfullmonth=1&dailytarget=10.00&topframedatabase=pithampur&frmclientid=pithampur&topframeusername=abhi&topframeuserid=8&topframeaccessrights=NYYYYNYNNNYNNYYNNNNNNNNNN&defaultlandingpage=&topframecustomer='
        response = requests.request("POST", url, headers=headers, data=payload)
        soup = BeautifulSoup(response.text, 'html.parser')
        try:
            table = soup.find('table')
            rows = table.find_all('tr')
            data = {}
            for row in rows:
                cols = row.find_all('td')
                if len(cols) == 0:
                    continue
                row_date = datetime.strptime(cols[0].text, "%a %d/%m/%Y")
                if row_date.date() == today:
                    data = {"UID": uuid['UID'], "KWH": float(cols[-1].text)}
                    break
            print("Done:", uid)
            return data
        except Exception as e:
            print(e)
            print("Error:", uuid)
            print(response.text)
            return {"UID": uuid['UID'], "KWH": 0}

def main():
    try:
        data = get_UIDs()
        kwh_data = [["Date", "Ward No", "Area Code", "Location Name", "SLC UID", "Connected Load KWH", "Operating Time", "Baseline KWH", "Adjusted Baseline KWH", "Actual Consuption KWH", "Actual Energy Savings KWH", "Actual Energy Savings %"]]
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            result = executor.map(get_kwh_fot_today_UID, data)
        results = list(result)
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            result = executor.map(get_operating_hours, data)
        operating_hours_list = list(result)
        for result in results:
            uid_data = list(filter(lambda x: x['UID'] == result['UID'], data))[0]
            operating_hour = list(filter(lambda x: x['UID'] == result['UID'], operating_hours_list))[0]['operating_hours']
            baseline = float(uid_data['Certified Baseline in kwh'])/11*operating_hour
            saved = baseline - float(result['KWH'])
            kwh_data.append([
                today,
                "NP",
                uid_data["Area Code"],
                uid_data["Location"],
                result['UID'],
                uid_data['Connected Load in kw'],
                str(int(operating_hour)) + ":"+ str(int((operating_hour%1)*60))+":00",
                uid_data['Certified Baseline in kwh'],
                baseline,
                result['KWH'],
                saved,
                saved*100/baseline
            
            ])
        areas = set(list(map(lambda x: x[2], kwh_data[1:])))
        files = []
        for area in areas: 
            filename = f"{area}_{today}.csv"   
            files.append(filename) 
            area_data = list(filter(lambda x: x[2] == area, kwh_data))
            if len(area_data) == 0:
                continue
            with open(filename, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(area_data)
        send_email(f"Energy Consumption Report for {today}", "pghhinfra@gmail.com", f"""Hello,\n\nPlease find attached the Energy Consumption Report for {today}.\n\nBest regards,\nPratik Automation AI""", files, ['pghhinfrastructure@gmail.com'])
    except Exception as e:
        err = traceback.format_exc()
        print(err)
        send_email("Error in Energy Consumption Report", "pratikdeshmukhlobhi@gmail.com", f"""Hello,\n\nThere was an error in generating the Energy Consumption Report for {today}.\n\n{str(err)}\n\nBest regards,\nPratik Automation AI""", [])
main()
# get_operating_hours("1001")