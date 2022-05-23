import requests
from bs4 import BeautifulSoup

resp = requests.get("https://www.cbr.ru/scripts/XML_daily.asp")  # Запрос к ЦБ
soap = BeautifulSoup(resp.content, "xml")  # Получение XML файла с последними котировками
bs_str_usd = soap.find('CharCode', text="USD").find_next_sibling("Value").string  # Получение суммы доллара
usd = round(float(bs_str_usd.replace(',', '.')),2)  # Перевод его в формат float
