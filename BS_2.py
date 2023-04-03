from bs4 import BeautifulSoup
import requests
from sqlalchemy import create_engine


def data_populator(row_dict):
    data = {}
    data["items"] = []
    for dish in row_dict["items"]:
        dishes = []
        for info in dish:
            try:
                dishes.append(info.text.replace(')', ' ').replace('"', ' ').replace('%', ' '))
            except AttributeError:
                dishes.append(info.replace('"', ' ').replace(')', ' ').replace('(', ' '))
        data["items"].append(dishes)

    for key in row_dict.keys():
        if key != "items":
            try:
                data[key] = row_dict[key].text.replace('%',' percent')
            except AttributeError:
                data[key] = row_dict[key]
    for item in data["items"]:
        connection.execute(F"""
        INSERT INTO restaurant_items (name, address, url, timings, no_of_reviews, deliver_rating, item_name, 
        item_cost, item_description, veg_non_veg) 
        VALUES("{data['name']}", "{data['address']}", "{data['url']}", "{data['timings']}", "{data['no_of_reviews']}", "{data['delivery_rating']}",
        "{item[0]}", "{item[1]}", "{item[2].replace('"', '').replace('%',' percent')}", "{item[3]}");        
        """)
        print("Inserted.")




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
select url from source where name not in (select name from restaurant_items);
""").fetchall()
a = 1
for row in result:
    print(row[0])
    print(a)
    link = f"https://www.zomato.com{row[0]}"
    agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64)'}
    response = requests.get(url=link, headers=agent)
    # print(response.text)
    # with open("pastries.txt", "w", encoding='utf-8') as file:
    #     file.write(response.text)
    soup = BeautifulSoup(response.text, "html.parser")
    row = {}
    row["name"] = soup.find(class_="sc-7kepeu-0 sc-iSDuPN fwzNdh")
    row["address"] = soup.find(class_="sc-clNaTc vNCcy")
    try:
        row["url"] = soup.find(class_="sc-clNaTc vNCcy")["href"]
    except TypeError:
        row["url"] = soup.find(class_="sc-clNaTc vNCcy")
    row["timings"] = soup.find(class_="sc-kasBVs dfwCXs")
    try:
        row["no_of_reviews"] = soup.find_all(class_="sc-1q7bklc-8 kEgyiI")[1]
    except IndexError:
        row["no_of_reviews"] = soup.find(class_="sc-1q7bklc-8 kEgyiI")
    row["delivery_rating"] = soup.find(class_="sc-1q7bklc-1 cILgox")
    row["items"] = []
    for item in soup.find_all(class_="sc-1s0saks-17 bGrnCu"):
        items = []
        items.append(item.find("h4"))
        items.append(item.find(class_="sc-17hyc2s-1 cCiQWA"))
        items.append(item.find(class_="sc-1s0saks-12 hcROsL"))
        try:
            items.append(item.find(class_="sc-1tx3445-0 kcsImg sc-1s0saks-0 jcidl")["type"])
        except TypeError:
            items.append(" ")
        row["items"].append(items)
    data_populator(row)