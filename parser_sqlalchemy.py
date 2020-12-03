from sqlalchemy import Column, Integer, String, Float, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from bs4 import BeautifulSoup
import requests
import re

engine = create_engine('sqlite:///orm.sqlite', echo=True)

Base = declarative_base()

class Moto(Base):
    __tablename__ = 'motos'
    id = Column(Integer, primary_key = True)
    moto_name = Column(String)
    moto_year = Column(Integer)

    def __init__(self, moto_name, moto_year):
        self.moto_name = moto_name
        self.moto_year = moto_year

    def __str__(self):
        return f'{self.id}, {self.moto_name}, {self.moto_year}'

class Price(Base):
    __tablename__ = 'price_01'
    id = Column(Integer, primary_key=True)
    moto_price = Column(Integer)

    def __init__(self, moto_price):
        self.moto_price = moto_price

    def __str__(self):
        return f'{self.id}, {self.moto_price}'

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

def site_parsing():

    max_page = 10
    pages = []

    for x in range(1, max_page + 1):
        pages.append(requests.get('https://moto.drom.ru/sale/+/Harley-Davidson+Softail/'))

    for n in pages:
        soup = BeautifulSoup(n.text, 'html.parser')

        moto_name = soup.find_all('a', class_="bulletinLink bull-item__self-link auto-shy")

        for rev in moto_name:
            a = str(rev.text)
            moto = re.split(r',', a)
            moto_name_s = str(moto[0])
            moto_year = re.sub(r'[ ]', '', moto[1])
            moto_year_s = int(moto_year)

            motos_obj = Moto(moto_name_s, moto_year_s)
            session.add(motos_obj)
            session.commit()


        price = soup.find_all('span', class_='price-block__price')
        pattern = r'(\d{1}\s\d{3}\s\d{3})|(\d{3}\s\d{3})'
        for rev in price:
            price_str = re.findall(pattern, rev.text)
            price_str = str(price_str)
            price_str = price_str.replace('\\xa0', '')
            price_str = re.sub(r"[\]['(),\s]", '', price_str)
            price_int = int(price_str)

site_parsing()

query = session.query(Moto, Price)

query = query.join(Moto, Moto.id == Price.id)
records = query.all()

for obj1, obj2 in records:
    print(obj1,obj2)