# Датасет "Індекси РДН України та сусідніх країн"
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta
import time
import random
import requests
from bs4 import BeautifulSoup

# Створимо порожній DataFrame і визначимо назви стовбчиків
result_df = pd.DataFrame(
    columns=['Дата', 'Тип навантаження', 'Країна', 'Ціна, євро/МВт*год', 'Різниця ціни, порівняно з Україною, %'])
date_end = datetime.today()  # Сьогоднішня дата
n = input("Вкажіть, за скільки місяців треба отримати дані: ")
# Знаючи за скільки місяців потрібні дані, можемо знайти початкову дату (date_start)
date_start = date_end - timedelta(days=30 * int(n))

# ПОчинаємо збирати дані.
date_range = pd.date_range(start=date_start, end=date_end)  # Діапазон дат
for cur_date in date_range:  # Проходимо по всіх датах в заданому діапазоні (від date_start до date_end)
    # Запит на отримання даних за поточну дату
    data = {'date': cur_date.strftime('%d.%m.%Y')}
    try:  # Якщо при запиті виникне помилка, то спробуємо ще раз (почекавши 4 секунди)
        response = requests.post(url='https://www.oree.com.ua/index.php/main/get_eu_prices', data=data)
    except:
        time.sleep(4)
        response = requests.post(url='https://www.oree.com.ua/index.php/main/get_eu_prices', data=data)
    f = response.text  # Отримаємо html сторінку
    soup = BeautifulSoup(f, 'html.parser')
    text = soup.get_text()  # Видаляємо теги
    text = text.replace(' ', '')  # Видаляємо пробіли
    # Залишимо лише по одному символу \n підряд
    while '\n\n' in text:
        text = text.replace('\n\n', '\n')
    # Розбиваємо рядок на список
    res = text.split('\n')
    if len(res) != 65:
        print(f'Помилка {data["date"]}: нетипова кількість даних')
        break
    cur_date = cur_date.date()
    # Створимо список з поточними даними які потім додамо до датафрейму
    new_rows = [[cur_date, 'база', 'Україна', res[4], ''],
                [cur_date, 'база', 'Польща', res[7], res[9]],
                [cur_date, 'база', 'Словаччина', res[11], res[13]],
                [cur_date, 'база', 'Угорщина', res[15], res[17]],
                [cur_date, 'база', 'Румунія', res[19], res[21]],
                [cur_date, 'пік', 'Україна', res[25], ''],
                [cur_date, 'пік', 'Польща', res[28], res[30]],
                [cur_date, 'пік', 'Словаччина', res[32], res[34]],
                [cur_date, 'пік', 'Угорщина', res[36], res[38]],
                [cur_date, 'пік', 'Румунія', res[40], res[42]],
                [cur_date, 'позапік', 'Україна', res[46], ''],
                [cur_date, 'позапік', 'Польща', res[49], res[51]],
                [cur_date, 'позапік', 'Словаччина', res[53], res[55]],
                [cur_date, 'позапік', 'Угорщина', res[57], res[59]],
                [cur_date, 'позапік', 'Румунія', res[61], res[63]]]
    # Додавання рядків до DataFrame
    for row in new_rows:
        result_df.loc[len(result_df)] = row
    print(cur_date.strftime('%d.%m.%Y'))
    # Зробимо паузу від 1 до 2 секунд
    time.sleep(random.randint(1, 2))
# В датафреймі result_df значення "" замінимо на Nan
result_df = result_df.replace("", np.nan)
# В стовбчику "Дата" перетворимо значення в тип datetime без часу
# result_df['Дата'] = pd.to_datetime(result_df['Дата'], format='%Y-%m-%d').dt.date
# В стовбчику "Різниця ціни, порівняно з Україною, %" видалимо символ % і переконвертуємо значення в float
result_df['Різниця ціни, порівняно з Україною, %'] = result_df['Різниця ціни, порівняно з Україною, %'].apply(
    lambda x: float(str(x).replace(',', '.').replace('%', '')))
# В стовбчику "Ціна, євро/МВт*год" значення ""
# В cтовбчику "Ціна, євро/МВт*год" переконвертуємо значення в float
result_df['Ціна, євро/МВт*год'] = result_df['Ціна, євро/МВт*год'].apply(lambda x: float(str(x).replace(',', '.')))
# З стовбчика "Дата" знайдемо мінімальне значення
date_start = result_df['Дата'].min()
# З стовбчика "Дата" знайдемо максимальне значення
date_end = result_df['Дата'].max()
# Збережемо результати в файл
result_df.to_excel(f'{date_end.strftime("%Y_%m_%d")}_іndeksy_rdn_ukraina_susidy.xlsx', index=False)
