#%%
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from clean_data import PollutionDataProcessor

START_DATE = "2010-01-01"


def scrape(url):
    # Fetch the HTML content
    response = requests.get(url)
    html_content = response.text

    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find('table', {'class': 'table table-condensed NOtable-responsive text-center'})
    date_pattern = r'\d{4}-\d{2}-\d{2}'
    match = re.search(date_pattern, url)
    rows = table.find_all('tr')[2:]  

    data = []
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        if cols:  
            data.append(cols)

    df = pd.DataFrame(data, columns=["Station", "SO2", "PM10", "PM2.5", "NO2", "CO", "O3", "C6H6"])
    df['Date'] = match.group(0)
    return df


existing_data = "milano_pollution_clean.parquet"
today = pd.Timestamp.today().strftime("%Y-%m-%d")


NEW = True

if existing_data in os.listdir("data"):
    NEW = False
    print("Existing data found")
    old_df = pd.read_parquet("data/" + existing_data)
    start_date = old_df.index.max()
else:
    print("No existing data found")
    start_date = START_DATE

print(f"Start date: {start_date}")



df = pd.DataFrame(columns=["Station", "SO2", "PM10", "PM2.5", "NO2", "CO", "O3", "C6H6", "Date"])

missing_dates = []
for date in pd.date_range(start=start_date, end=today):
    print(date)
    try:
        url = f'https://www.amat-mi.it/index.php?id_sezione=35&data_bollettino={date.strftime("%Y-%m-%d")}'
        day_df = scrape(url)
        df = pd.concat([df, day_df])

        # print first day of the year
        if date.day == 1 and date.month == 1:
            print(date)

    except:
        missing_dates.append(date)

    
processor = PollutionDataProcessor(df)
cleaned_df = processor.process()


if existing_data:
    print("Concatenating old data")
    df = pd.concat([old_df, cleaned_df])

# df.to_parquet('data/milano_pollution_clean.parquet')
