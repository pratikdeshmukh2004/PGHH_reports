import requests
from bs4 import BeautifulSoup
from pprint import pprint
from datetime import datetime, date, timedelta
import openpyxl
import concurrent.futures, csv
from mail import send_email
import traceback, os, json
from calendar import monthrange

today = date.today()

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
months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
    

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
    if "Today" in uuid:
        selected_date = uuid['Today']
    else:
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

def get_operating_hours_monthly(uuid):
    num_days = monthrange(today.year, today.month)[1]
    days = [datetime(today.year, today.month, day).strftime("%Y-%m-%d") for day in range(1, num_days + 1)]
    op_list = []
    for i in days:
        uuid['Today'] = i
        op_list.append(uuid)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        result = executor.map(get_operating_hours, op_list)
    results = list(result)
    operating_hours = sum(list(map(lambda x: x['operating_hours'], results)))
    return {"UID": uuid['UID'], "operating_hours": operating_hours/len(results)}

def get_burning_load_monthly(uuid):
    uid = str(uuid['UID']).split("-")[-1].strip()
    print("Fetching burning load:", uid)
    start_from = datetime(today.year, today.month, 1)
    end_to = datetime(today.year, today.month, monthrange(today.year, today.month)[1]).strftime("%Y-%m-%d")
    url = f"https://www.powerz.in/powerz/kwhreports/showcurrentreading.php?paramselect=-1&datevalfrom={start_from}&datevalto={end_to}%2023:59:59&meter_primary_id={uid}&db=pithampur&_=1718957168715"
    response = requests.get(url, headers=headers)
    burnings = response.json()['data']
    burning_loads = {}
    for burning in burnings:
        try:
            if burning[0][:8] not in burning_loads:
                burning_loads[burning[0][:8]] = float(burning[-4])/1000
            else:
                if float(burning[-4])/1000 > burning_loads[burning[0][:8]]:
                    burning_loads[burning[0][:8]] = float(burning[-4])/1000
        except:
            pass
    return {"UID": uuid['UID'], "burning_load": sum(burning_loads.values())}

def get_monthly_consumption(uuid):
    uid = str(uuid['UID']).split("-")[-1].strip()
    print("Fetching:", uid)
    url = f"https://www.powerz.in/powerz/kwhreports/rptdaily.php?page=m&elw=1&mid={uid}&savedailytarget=1"
    currentweekval = get_current_week()
    payload = f'groupid=&meteridsel=1001&curweekval={currentweekval}&showfullmonth=1&dailytarget=10.00&topframedatabase=pithampur&frmclientid=pithampur&topframeusername=abhi&topframeuserid=8&topframeaccessrights=NYYYYNYNNNYNNYYNNNNNNNNNN&defaultlandingpage=&topframecustomer='
    response = requests.request("POST", url, headers=headers, data=payload)
    soup = BeautifulSoup(response.text, 'html.parser')
    data = {"UID": uuid['UID'], "KWH": 0}
    try:
        table = soup.find('table')
        rows = table.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            if len(cols) == 0:
                continue
            row_date = datetime.strptime(cols[0].text, "%a %d/%m/%Y")
            if row_date.date().month == today.month:
                data["KWH"] += float(cols[-1].text)
        print("Done:", uid)
        return data
    except Exception as e:
        print(e)
        print("Error:", uuid)
        print(response.text)
        return data

def send_monthly_report():
    try:
        data = get_UIDs()[:2]
        _, days_in_month = monthrange(today.year, today.month)
        kwh_data = [["Date", "Ward No", "Area Code", "Location Name", "SLC UID", "Connected Load (kW)", "Operating Time (HH:MM)", "Operating Time (In Decimal)", "Operating Time (%)", "Base Load (kW)", "Burning Load (kW)", "Correction Factor", "Baseline (kWh)", "Adjusted Baseline (kWh)", "Actual Consumption (kWh)", "Actual Energy Savings (kWh)","Actual Energy Savings (%)"]]
        kwh_data2 = [["Area Code",'Connected Load (kW)', 'Baseline (kWh)', 'Adjusted Baseline (kWh)', 'Actual Consumption (kWh)', 'Actual Energy Savings (kWh)', 'Actual Energy Savings (%)', 'Allocated Energy Savings (kWh)', 'Additional Energy Savings (kWh)']]
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            result = executor.map(get_monthly_consumption, data)
        results = list(result)
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            result = executor.map(get_operating_hours_monthly, data)
        operating_hours_list = list(result)
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            result = executor.map(get_burning_load_monthly, data)
        burning_load_list = list(result)
        for result in results:
            uid_data = list(filter(lambda x: x['UID'] == result['UID'], data))[0]
            connected_load = float(uid_data['Connected Load in kw'])*days_in_month
            burning_load = list(filter(lambda x: x['UID'] == result['UID'], burning_load_list))[0]['burning_load'] 
            certified_baseline = float(uid_data['Certified Baseline in kwh'])*days_in_month
            operating_hour = list(filter(lambda x: x['UID'] == result['UID'], operating_hours_list))[0]['operating_hours']
            correction_factor = burning_load/connected_load
            baseline = certified_baseline*correction_factor*(operating_hour/11)
            saved = baseline - float(result['KWH'])
            kwh_data.append([
                today,
                "NP",
                uid_data["Area Code"],
                uid_data["Location"],
                result['UID'],
                connected_load,
                str(int(operating_hour)) + ":"+ str(int((operating_hour%1)*60))+":00",
                operating_hour,
                operating_hour/11,
                connected_load,
                burning_load,
                correction_factor,
                certified_baseline,
                baseline,
                result['KWH'],
                saved,
                (saved if saved > 0 else 1)/(baseline if baseline > 0 else 1)
            
            ])
            kwh_data2.append([
                uid_data["Area Code"],
                connected_load,
                certified_baseline,
                baseline,
                result['KWH'],
                saved,
                (saved if saved > 0 else 1)/(baseline if baseline > 0 else 1),
            ])
        areas = set(list(map(lambda x: x[2], kwh_data[1:])))
        files = []
        for area in areas: 
            # Report file...
            filename = f"{area}_{months[today.month-1]}.csv"   
            files.append(filename) 
            area_data = list(filter(lambda x: x[2] == area or kwh_data.index(x) == 0, kwh_data))
            if len(area_data) == 0:
                continue
            with open(filename, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(area_data)
            
            # Summary file...
            filename = f"{area}_{months[today.month-1]}_Summary.csv"
            files.append(filename)
            area_data = list(filter(lambda x: x[0] == area or kwh_data2.index(x) == 0, kwh_data2))
            if len(area_data) == 0:
                continue
            with open(filename, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(area_data)
        # send_email(f"Energy Consumption Report for {today}", "pghhinfra@gmail.com", f"""Hello,\n\nPlease find attached the Energy Consumption Report for {today}.\n\nBest regards,\nPratik Automation AI""", files, ['pghhinfrastructure@gmail.com'])
        send_email(f"Energy Consumption Report for {months[today.month-1]}", "pratikdeshmukhlobhi@gmail.com", f"""Hello,\n\nPlease find attached the Energy Consumption Report for {months[today.month-1]}.\n\nBest regards,\nPratik Automation AI""", files)
    except Exception as e:
        err = traceback.format_exc()
        print(err)
        send_email("Error in Energy Consumption Report", "pratikdeshmukhlobhi@gmail.com", f"""Hello,\n\nThere was an error in generating the Energy Consumption Report for {months[today.month-1]}.\n\n{str(err)}\n\nBest regards,\nPratik Automation AI""", [])


if __name__ == "__main__":
    print("----------------- Generating Monthly Report -----------------")
    send_monthly_report()
