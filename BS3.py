from bs4 import BeautifulSoup
import requests
import re
from json import loads
from sqlalchemy import create_engine


def json_Extractor(page_link):
    agent = {"User-Agent": 'Mozilla/5.0 (Windows NT 6.3; WOW64)'}
    response = requests.get(url=page_link, headers=agent)
    soup = BeautifulSoup(response.text, 'html.parser')
    if re.search(r"JSON.parse\((.*?)\)\;", soup.prettify()) is None:
        return {'pages': {'current': {'name': '404'}}}
    double_decoded_json = re.search(r"JSON.parse\((.*?)\)\;", soup.prettify()).group(1)
    decoded_json = loads(double_decoded_json)
    return loads(decoded_json)


# def remove_HTML_tags(comment):
#     re_pattern = re.compile('<.*?>')
#     cleantext = re.sub(, '', comment)
#     return cleantext


conn_str = "mysql+pymysql://{user}:{password}@{host}:{port}/{database}".format(
    user="root",
    password="password",
    host="localhost",
    port="3306",
    database="zomato_main"
)
# create the engine
engine = create_engine(conn_str)

# connect to the database
connection = engine.connect()

result = connection.execute("""
select url from source where name not in (select restaurant_name from restaurant_reviews) order by url desc;
""").fetchall()
for row in result:
    source_link = f'https://www.zomato.com{row[0].replace("order", "")}reviews'
    print(source_link)
    json_data = json_Extractor(source_link)
    if json_data['pages']['current']['name'] != '404':
        key = list(json_data['pages']['restaurant'].keys())[0]
        number_of_pages = json_data['pages']['restaurant'][key]['sections']['SECTION_REVIEWS']['numberOfPages']
        restaurant_name = json_data['pages']['restaurant'][key]['sections']['SECTION_BASIC_INFO']['name']
        if number_of_pages > 5:
            number_of_pages = 5
        for i in range(1, number_of_pages + 1):
            link = f'{source_link}?page={i}&sort=dd&filter=reviews-dd'
            json_data = json_Extractor(link)
            reviews = json_data["entities"]['REVIEWS']
            for user in list(reviews.keys()):
                username = reviews[user]['userName']
                review = reviews[user]['reviewText'].replace('%', "percent").replace('"', "").replace(')', ' ').replace(
                    '(', ' ')
                type = reviews[user]['ratingV2Text']
                rating = reviews[user]['ratingV2']
                timestamp = reviews[user]['timestamp']
                connection.execute(f"""
                INSERT INTO restaurant_reviews (restaurant_name, user_name, reviews, timestamp, ratings, type)
                VALUES ("{restaurant_name}", "{username}", "{review}", "{timestamp}",
                "{rating}", "{type}");
                """)
                print("Inserted.")
