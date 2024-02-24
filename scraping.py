#%%
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

def scrape(url):
    # Fetch the HTML content
    response = requests.get(url)
    html_content = response.text

    soup = BeautifulSoup(html_content, 'html.parser')

    # Find the table
    table = soup.find('table', {'class': 'table table-condensed NOtable-responsive text-center'})

    date_pattern = r'\d{4}-\d{2}-\d{2}'

    # Search for the date in the URL
    match = re.search(date_pattern, url)

    # Extracting data
    rows = table.find_all('tr')[2:]  

    # Parsing table data
    data = []
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        if cols:  # Exclude empty rows
            data.append(cols)

    # Creating a DataFrame
    df = pd.DataFrame(data, columns=["Station", "SO2", "PM10", "PM2.5", "NO2", "CO", "O3", "C6H6"])
    df['Date'] = match.group(0)
    # Displaying the DataFrame
    return df

# start from 2024-01-01
start_date = '2010-01-01'
today = pd.Timestamp.today().strftime("%Y-%m-%d")


df = pd.DataFrame(columns=["Station", "SO2", "PM10", "PM2.5", "NO2", "CO", "O3", "C6H6", "Date"])

missing_dates = []
for date in pd.date_range(start=start_date, end=today):
    try:
        url = f'https://www.amat-mi.it/index.php?id_sezione=35&data_bollettino={date.strftime("%Y-%m-%d")}'
        day_df = scrape(url)
        df = pd.concat([df, day_df])

        # print first day of the year
        if date.day == 1 and date.month == 1:
            print(date)

    except:
        missing_dates.append(date)


df.to_parquet('data/milano_pollution.parquet')

