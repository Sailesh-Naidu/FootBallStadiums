import googlemaps
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from datetime import datetime

NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'

def get_wikipedia_page(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"An error occurred with exception : {e}")


def get_wikipedia_data(html):
    soup = BeautifulSoup(html,'html.parser')
    tables = soup.select("table.wikitable.sortable")
    if not tables:
        raise ValueError('No table found')
    table = tables[0]
    rows = table.find_all('tr')
    return rows

def extract_wikipedia_data(**kwargs):
    url = kwargs['url']
    html = get_wikipedia_page(url)
    rows = get_wikipedia_data(html)
    data = []
    for i in range (1, len(rows)):
        tds = rows[i].find_all('td')
        values = {
            'rank': i,
            'stadium' : clean_text(tds[0].text),
            'capacity' : clean_text(tds[1].text).replace(',',''),
            'regions': clean_text(tds[2].text),
            'country' : clean_text(tds[3].text),
            'city': clean_text(tds[4].text),
            'images' : 'https://' + tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else 'No image',
            'home_team' : clean_text(tds[6].text),
        }
        data.append(values)
    json_rows = json.dumps(data)
    kwargs['ti'].xcom_push(key='rows', value = json_rows)
    return 'OK'


def clean_text(text):
    text = str(text).strip()
    text.replace('&nbsp', '')
    if text.find(' ♦'):
        text = text.split(' ♦')[0]
    if text.find('[')!=-1:
        text = text.split('[')[0]
    if text.find(' (formerly)') != -1:
        text = text.split(' (formerly)')[0]
    return text.replace('\n','')

import time

gmaps = googlemaps.Client(key='AIzaSyCg0t9E84YmijH20vCEULjPKMZMYy6cits')

def get_lat_long(city, stadium):
    query = f"{stadium}, {city}"
    geocode_result = gmaps.geocode(query)
    if geocode_result:
        location = geocode_result[0]['geometry']['location']
        return location['lat'], location['lng']
    return None

def transform_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows',task_ids='extract_data_from_wiki')
    data = json.loads(data)
    stadiums_df = pd.DataFrame(data)
    stadiums_df['location'] = stadiums_df.apply(lambda x: get_lat_long(x['country'],x['stadium']), axis=1)
    stadiums_df['images'] = stadiums_df['images'].apply(lambda x: x if x not in ['No image', '', None] else NO_IMAGE)
    stadiums_df['capacity'] = stadiums_df['capacity'].astype(int)

    #handle duplicate data
    duplicates = stadiums_df[stadiums_df.duplicated(['location'])]
    duplicates['location'] = duplicates.apply(lambda x : get_lat_long(x['country'],x['city']), axis =1)
    stadiums_df.update(duplicates)
    kwargs['ti'].xcom_push(key='updated_rows', value=stadiums_df.to_json())
    return "OK"

def write_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='updated_rows', task_ids='transform_fetched_data')
    data = json.loads(data)
    data = pd.DataFrame(data)
    file_name = ('stadium_cleaned_' + str(datetime.now().date())
                 + "_" + str(datetime.now().time()).replace(":", "_") + '.csv')
    data.to_csv('data/' + file_name, index=False)
