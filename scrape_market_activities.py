import requests # Get URL data
from bs4 import BeautifulSoup # Manipulate URL data
from pymongo import MongoClient
from datetime import timedelta, date
import os


# ad_decline takes a row of data from the JSE Market Activity list and converts it
# into a list of dictionaries formatted like:
# {ticker symbol, stock name, stock volume, closing price, percentage change}

def ad_decline(rows):
    data_list = []
    ticker_list = []
    name_list = []
    volume_list = []
    c_price_list = []
    p_change_list = []
    change_list = []
    for row in rows:
        data = row.find_all('td')
        ticker_list.append(data[0].text.strip())
        name_list.append(data[1].text.strip())
        volume_list.append(data[2].text.strip())
        c_price_list.append(data[3].text.strip())
        p_change_list.append(data[4].text.strip())
        change_list.append(data[5].text.strip())
        

    for i in range(len(ticker_list)):
        data_dict = {
            "ticker": ticker_list[i],
            "name": name_list[i],
            "volume": volume_list[i],
            "c_price": c_price_list[i],
            "price_change": p_change_list[i],
            "change": change_list[i]
        }
        data_list.append(data_dict)

    return(data_list)

# trading_firm takes a row of data from the JSE Market Activity list and converts it
# into a list of dictionaries formatted like:
# {ticker symbol, stock name, stock volume, closing price = 0, percentage change = 0}

def trading_firm(rows):
    data_list = []
    ticker_list = []
    name_list = []
    volume_list = []
    c_price_list = []
    for row in rows:
        data = row.find_all('td')
        ticker_list.append(data[0].text.strip())
        name_list.append(data[1].text.strip())
        volume_list.append(data[2].text.strip())
        c_price_list.append(data[3].text.strip())
        

    for i in range(len(ticker_list)):
        data_dict = {
            "ticker": ticker_list[i],
            "name": name_list[i],
            "volume": volume_list[i],
            "c_price": c_price_list[i],
            "price_change": "0",
            "change": "0"
        }
        data_list.append(data_dict)

    return(data_list)


# Scrapes the activity data and seperates them into three sets of table rows

def scrapeNews(date):
    url_string = "https://www.jamstockex.com/trading/trade-summary/?market=combined-market&date="
    test_page = requests.get(url_string + date)
    soup = BeautifulSoup(test_page.text, "html.parser")
    soup.prettify() # this gives HTML text for a given web page
    f = open("tabledata.txt", "a")

    try:
        p_items = soup.find_all("tbody")
        a_rows = p_items[0].find_all('tr')
        d_rows = p_items[1].find_all('tr')
        t_rows = p_items[2].find_all('tr')

        advancing = ad_decline(a_rows)
        declining = ad_decline(d_rows)
        firm = trading_firm(t_rows)

        data_list = {
            "advancing": advancing,
            "declining": declining,
            "trading_firm": firm,
            "date": date
        }

        stock_list = []
        for i in advancing:
            stock_list.append(i)
        for i in declining:
            stock_list.append(i)
        for i in firm:
            stock_list.append(i)

        outcome = date + " has been successfully scraped"
        print(outcome)
        return data_list
    
    except:
        # If there is no data it was potentially and holiday and should be skipped
        print("Potentially Holiday")
    return []






# Provides a list of dates inclusive of start and end date
def daterange(date1, date2):
    for n in range(int ((date2 - date1).days)+1):
        yield date1 + timedelta(n)

start_dt = date(2022,5,16)
end_dt = date(2022,5,17)

date_list = []
weekdays = [6,7]

# Removes weekdays from the date range
for dt in daterange(start_dt, end_dt):
    if dt.isoweekday() not in weekdays:
        date_list.append(dt.strftime("%Y-%m-%d"))



# Creates all of the list 
def all_activity():

    activity_list = []
    for i in date_list:
        info = scrapeNews(i)
        if info != []:
            activity_list.append(info)

    return activity_list


# Insert Activities into the Activity Collection of MongoDB
def insert_activities():
    try:
        client = MongoClient(os.environ.get("DATABASE_URI"))    
        print("Connection Successful")
        db = client.database

        activities = db.activities

        activity_list = all_activity()

        for i in activity_list:
            activities.insert_one(i)

        print("Successfully added activities")

    except:
        print("Connection Failed")
    
   

insert_activities()


