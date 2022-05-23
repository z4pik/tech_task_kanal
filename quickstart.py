import time
import time
from pprint import pprint
import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
import psycopg2
from config import host, user, password, db_name
from xml_parsing import usd


# Перерыв в работе программы
time_break = 10

# Создание соединения с БД
connection = psycopg2.connect(host=host, user=user, password=password, dbname=db_name)

while True:
    try:
        """Скрипт получающий данные с Sheets"""
        # Файл, полученный в Google Developer Console
        CREDENTIALS_FILE = 'creds.json'
        # ID Google Sheets документа (можно взять из его URL)
        spreadsheet_id = '1Q2zM7uwFTXDzi8i02Ix1JTd1a5OJayq2lXPPXxvEUe4'

        # Авторизуемся и получаем service — экземпляр доступа к API
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            CREDENTIALS_FILE,
            ['https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive'])
        httpAuth = credentials.authorize(httplib2.Http())
        service = apiclient.discovery.build('sheets', 'v4', http=httpAuth)

        # Пример чтения файла
        values = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range='A2:E1000',
            majorDimension='ROWS'
        ).execute()

        thisdict = values["values"]

        # Сохранение изменений
        connection.autocommit = True

        # Получение каждой строки отдельно
        for i in range(0, len(thisdict)):
            data = thisdict[i]
            id = data[0]
            order_num = data[1]
            dollar_sum = data[2]
            rub_sum = str(int(dollar_sum)*usd)
            delivery_time = data[3]
            try:
                """
                    Как работает скрипт:
                    Он загружает данные в промежуточную таблицу, после чего копирует х в основную таблицу
                    затем очищает промежуточную таблицу.
                    
                    Если же нужно внести изменения нужно раскоментировать "очищение первой таблицы", 
                    после выполнения скрипта поставить коментарий обратно.
                """

                # Создание таблицы
                # with connection.cursor() as cursor:
                #     cursor.execute(
                #         """CREATE TABLE orders(
                #             id integer,
                #             order_num integer NOT NULL,
                #             dollar_sum integer NOT NULL,
                #             delivery_date date,
                #             rub_sum decimal(10,2));"""
                #     )

                # Очищене первой таблици
                with connection.cursor() as cursor:
                    cursor.execute(
                        """TRUNCATE orders;
                            DELETE FROM orders;"""
                    )
                    print(f"Удаление записи с id = {id}")

                # Внесеные данных в таблицу
                with connection.cursor() as cursor:
                    cursor.execute(
                        """INSERT INTO orders2 (id, order_num, dollar_sum, delivery_date, rub_sum)
                        VALUES (%s, %s, %s, %s, %s) """, (id, order_num, dollar_sum, delivery_time, rub_sum)
                    )
                    print("[INFO] Data was succefully inserted")

                # Загрузка данных в основную таблицу
                with connection.cursor() as cursor:
                    cursor.execute(
                        """INSERT INTO orders SELECT * FROM orders2;"""
                    )
                    print(f"Добавление записи с id = {id}")

                # Очищение 2 таблицы
                with connection.cursor() as cursor:
                    cursor.execute(
                        """TRUNCATE orders2;
                            DELETE FROM orders2;"""
                    )
                    print(f"Удаление записи с id = {id}")

            except Exception as _ex:
                print("[INFO] Error while working with PostgreSQL", _ex)
        time.sleep(time_break)
        # if connection:
        #     # cursor.close()
        #     connection.close()
        #     print("[INFO] PostgreSQL connection closed")

    except:
        pass
