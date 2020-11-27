from bs4 import BeautifulSoup
import requests
import re

def site_parsing_2():

    max_page = 10
    pages = []

    moto_name_list = []
    moto_year_list = []
    price_list = []
    price_list_len = []
    moto_list_len = []


    for x in range(1, max_page + 1):
        pages.append(requests.get('https://moto.drom.ru/sale/+/Harley-Davidson+Softail/'))

    for n in pages:
        soup = BeautifulSoup(n.text, 'html.parser')

        moto_name = soup.find_all('a', class_="bulletinLink bull-item__self-link auto-shy")

        for rev in moto_name:
            a = str(rev.text)
            moto_list_len.append(a)
            moto = re.split(r',', a)
            moto_name_list.append(moto[0])
            moto_year = re.sub(r'[ ]', '', moto[1])
            moto_year_list.append(moto_year)


        price = soup.find_all('span', class_ = 'price-block__price')
        pattern = r'(\d{1}\s\d{3}\s\d{3})|(\d{3}\s\d{3})'
        for rev in price:
            b = rev.text
            price_list_len.append(b)
            price_str = re.findall(pattern, rev.text)
            price_str = str(price_str)
            price_str = price_str.replace('\\xa0', '')
            price_str = re.sub(r"[\]['(),\s]", '', price_str)
            price_list.append(price_str)

    price_list_int = []

    for el in price_list:
        price_int = int(el)
        price_list_int.append(price_int)

    i = 0
    for x in range(len(price_list_int)):
        i += price_list_int[x]

    moto_name_site = moto_name_list[0]
    site_name = 'https://moto.drom.ru/sale'
    average_price = round(i / len(price_list_int))
    min_price = min(price_list_int)
    max_price = max(price_list_int)
    offers_all = len(moto_name_list[::2])

    data = dict(moto_name_site = moto_name_site, site_name = site_name, average_price = average_price, max_price = max_price,
                min_price = min_price, offers_all = offers_all)

    return data

    print(data)