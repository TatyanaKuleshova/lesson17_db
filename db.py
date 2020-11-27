import sqlite3 as lite
import sys
from bs4 import BeautifulSoup
import requests
import re

def site_parsing():

    max_page = 10
    pages = []

    id_n = 0
    id_n_price = 0

    for x in range(1, max_page + 1):
        pages.append(requests.get('https://moto.drom.ru/sale/+/Harley-Davidson+Softail/'))

    for n in pages:
        soup = BeautifulSoup(n.text, 'html.parser')

        moto_name = soup.find_all('a', class_="bulletinLink bull-item__self-link auto-shy")

        for rev in moto_name:
            id_n += 1
            a = str(rev.text)
            moto = re.split(r',', a)
            moto_name_s = str(moto[0])
            moto_year = re.sub(r'[ ]', '', moto[1])
            moto_year_s = int(moto_year)
            cur.execute("INSERT INTO moto VALUES(?,?,?)", (id_n, moto_name_s, moto_year_s))

        price = soup.find_all('span', class_='price-block__price')
        pattern = r'(\d{1}\s\d{3}\s\d{3})|(\d{3}\s\d{3})'
        for rev in price:
            id_n_price += 1
            price_str = re.findall(pattern, rev.text)
            price_str = str(price_str)
            price_str = price_str.replace('\\xa0', '')
            price_str = re.sub(r"[\]['(),\s]", '', price_str)
            price_int = int(price_str)
            cur.execute("INSERT INTO moto_price VALUES(?,?)", (id_n_price, price_int))


connect = None

try:
    connect = lite.connect('motos.db')
    cur = connect.cursor()
    cur.execute("CREATE TABLE moto(id INT, moto TEXT, year INT)")
    cur.execute("CREATE TABLE moto_price(id INT, price INT)")

    site_parsing()

except lite.Error as e:
    print(f"Error {e.args[0]}:")
    sys.exit()

with connect:
    cur = connect.cursor()

    rows_join = f'SELECT * FROM moto JOIN moto_price ON moto.id = moto_price.id'

    cur.execute(rows_join)
    rows = cur.fetchall()
    for row in rows:
        print(row)

connect.close()
