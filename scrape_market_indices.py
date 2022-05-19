from calendar import c
import requests # Get URL data
from bs4 import BeautifulSoup # Manipulate URL data
import datetime # useful since stock prices are IRL time-series data
import os
from pymongo import MongoClient

# Scrapes indices data from each market over a period of time

def scrapeIndices(code, start_date, end_date):
    url_start_string = "https://www.jamstockex.com/trading/indices/index-history/?indexCode="
    url_code_string = url_start_string + code
    url = url_code_string +"&fromDate="+ start_date + "&thruDate=" + end_date
    test_page = requests.get(url)
    soup = BeautifulSoup(test_page.text, "html.parser")
    soup.prettify() # this gives HTML text for a given web page

    p_items = soup.find_all("tbody")
    rows = p_items[0].find_all('tr')

    data_list = []

    date_list = []
    points_list = []
    change_list = []
    c_percent_list = []
    volume_list = []
    for row in rows:
        data = row.find_all('td')
        date_list.append(data[0].text.strip())
        points_list.append(data[1].text.strip())
        change_list.append(data[2].text.strip())
        c_percent_list.append(data[3].text.strip())
        volume_list.append(data[4].text.strip())
    

    for i in range(len(date_list)):
        date = datetime.datetime.strptime(date_list[i], '%b-%d-%Y').strftime('%Y-%m-%d')
        
        ls= {
            "date": date,
            "points": points_list[i],
            "change": change_list[i],
            "change_percent": c_percent_list[i],
            "volume": volume_list[i]
        }
        data_list.append(ls)

    return data_list


indices_list = []


# Seperates the market data into list based on the market type
def all_markets():
    markets = ["jse-index","jse-junior","combined-index", "us-equities", "financial-index", "manufactoring-index"]
    labels = ["main", "junior", "combined","us","financial","manufacturing"]

    start_date = "2022-05-16"
    end_date = "2022-05-17"
    
    

    for i in range(len(markets)):
        index_info = {labels[i] : scrapeIndices(markets[i], start_date, end_date)}
        indices_list.append(index_info)

    new_date_list = [i["date"] for i in indices_list[0]["main"]]
    in_list = []
    for i in range(len(new_date_list)):
        index_info = {}
        for x in range(len(labels)):
            index_info.update({labels[x]: indices_list[x][labels[x]][i]})

        in_info = {
            "date": new_date_list[i], 
            "index_info": index_info
        }
        
        in_list.append(in_info)



    return in_list
    


def insert_indices():
    try:
        client = MongoClient(os.environ.get("DATABASE_URI"))    
        print("Connection Successful")

        db = client.database

        indices = db.indices
    
        in_list = all_markets()

        for i in in_list:
            indices.insert_one(i)

    except:
        print("Connection Failed")
    


insert_indices()