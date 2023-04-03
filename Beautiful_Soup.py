from bs4 import BeautifulSoup
from sqlalchemy import create_engine


def data_populator(row_dict):
    data = {}
    for key in row_dict.keys():
        if row_dict[key]:
            try:
                data[key] = row_dict[key].text.replace("%", "percent")
            except AttributeError:
                data[key] = row_dict[key]
        else:
            data[key] = "NULL"

    connection.execute(f"""
    INSERT INTO source (name, url, discount, categories, cost, orders_placed, stars, timings) 
    VALUES ("{data["name"]}", "{data["url"]}", "{data["discount"]}", "{data["categories"]}", "{data["cost"]}", 
    "{data["orders_placed"]}", "{data["stars"]}", "{data["timings"]}");
    """)


conn_str = "mysql+pymysql://{user}:{password}@{host}:{port}/{database}".format(
    user="root",
    password="password",
    host="localhost",
    port="3306",
    database="zomato_main"
)
engine = create_engine(conn_str)
connection = engine.connect()
file_path = r"C:\Users\mithu\Documents\zomato_body.txt"
html_doc = open(file_path, "r", encoding='utf-8').read()
soup = BeautifulSoup(html_doc, 'html.parser')
for restaurant in soup.find_all(class_="sc-ZUflv efYsXh"):
    row = {}
    if restaurant:
        #restaurant url
        row["url"] = restaurant.find("a")['href']
        #Discount
        row["discount"] = restaurant.find(class_="sc-1hez2tp-0")
        #Hotel name
        row["name"] = restaurant.find("h4")
        #stars
        row["stars"] = restaurant.find(class_="sc-1q7bklc-1")
        #Categories
        row["categories"] = restaurant.find(class_="sc-1hez2tp-0 sc-LAuEU fDjWNG")
        #cost
        row["cost"] = restaurant.find(class_="sc-1hez2tp-0 sc-LAuEU dHtbEm")
        #safety measure
        row["orders_placed"] = restaurant.find(class_="sc-1hez2tp-0 sc-gUlUPW kFZReH")
        #Timings
        row["timings"] = restaurant.find(class_="sc-1hez2tp-0 sc-ljUfdc ipXDgX")
        print(row)
        data_populator(row)











