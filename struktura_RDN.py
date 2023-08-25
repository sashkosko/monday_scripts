# Датасет " Структура продажу та купівлі електроенергії на РДН"
import pandas as pd
from datetime import datetime
from datetime import timedelta
import time
import random
import requests

# Створимо порожній DataFrame
result_df = pd.DataFrame()
date_end = datetime.today()  # Сьогоднішня дата
n = input("Вкажіть, за скільки місяців треба отримати дані: ")
# Знаючи за скільки місяців потрібні дані, можемо знайти початкову дату (date_start)
date_start = date_end - timedelta(days=30 * int(n))

# ПОчинаємо збирати дані.
date_range = pd.date_range(start=date_start, end=date_end)  # Діапазон дат
for cur_date in date_range:  # Проходимо по всіх датах в заданому діапазоні (від date_start до date_end)
    # Запит на отримання даних за поточну дату
    res = requests.post('https://www.oree.com.ua/index.php/IDM_graphs/get_data_char1_ips_buos',
                        data={'date': cur_date.strftime('%d.%m.%Y')})
    json_data = [res.json()]

    df = pd.json_normalize(json_data, record_path=None, meta=None, errors='raise', record_prefix=None, sep='_')

    # ---Продаж---
    # Розгортаємо елементи які є списками (кожен елемент списку стає окремим рядком)
    expanded_data = []
    for index, row in df.iterrows():
        for labch, accept, accept_percent, objem, objem_percent, apaa in zip(row['labch4'], row['accept'],
                                                                             row['accept_percent'], row['objem'],
                                                                             row['objem_percent'], row['apaa']):
            expanded_data.append(
                {'labch': labch, 'Обсяг акцептованої': accept, 'Частка акцептованої': accept_percent,
                 'Обсяг заявленої': objem, 'Частка заявленої': objem_percent, 'apaa': apaa})
    # Створюємо DataFrame з розгорнутими даними
    p_df = pd.DataFrame(expanded_data)

    # Розділимо стовбчик 'labch' на два стовбчики. Розділювачем буде крапка з комою
    p_df[['Дата', 'Суб\'єкт']] = p_df['labch'].str.split(';', expand=True)
    # Розділимо стовбчик 'apaa' на два стовбчики. Розділювачем буде крапка з комою
    p_df[['Середньозважена ціна', 'Частка']] = p_df['apaa'].str.split(';', expand=True)
    # Додамо стовбчик "Вид діяльності" із значенням "Продаж"
    p_df['Вид діяльності'] = 'Продаж'
    # Переконвертуємо стовбчик "Дата" із стрічкового типу у тип datetime без часу
    p_df['Дата'] = pd.to_datetime(p_df['Дата'], format='%d.%m.%Y').dt.date
    # Залишимо лише рядки, де дата дорівнює [поточній даті - 1 день]
    p_df = p_df[p_df['Дата'] == (cur_date - timedelta(days=1)).date()]

    # ---Купівля---
    # Розгортаємо елементи які є списками (кожен елемент списку стає окремим рядком)
    expanded_data = []
    for index, row in df.iterrows():
        for labch, accept, accept_percent, objem, objem_percent, apaa in zip(row['labch5'], row['accept5'],
                                                                             row['accept5_percent'], row['objem5'],
                                                                             row['objem5_percent'], row['apaa5']):
            expanded_data.append(
                {'labch': labch, 'Обсяг акцептованої': accept, 'Частка акцептованої': accept_percent,
                 'Обсяг заявленої': objem, 'Частка заявленої': objem_percent, 'apaa': apaa})
    # Створюємо DataFrame з розгорнутими даними
    k_df = pd.DataFrame(expanded_data)

    # Розділимо стовбчик 'labch' на два стовбчики. Розділювачем буде крапка з комою
    k_df[['Дата', 'Суб\'єкт']] = k_df['labch'].str.split(';', expand=True)
    # Розділимо стовбчик 'apaa' на два стовбчики "sered" і "chastka". Розділювачем буде крапка з комою
    k_df[['Середньозважена ціна', 'Частка']] = k_df['apaa'].str.split(';', expand=True)
    # Додамо стовбчик "Вид діяльності" із значенням "Продаж"
    k_df['Вид діяльності'] = 'Купівля'
    # Переконвертуємо стовбчик "Дата" із стрічкового типу у тип datetime без часу
    k_df['Дата'] = pd.to_datetime(k_df['Дата'], format='%d.%m.%Y').dt.date
    # Залишимо лише рядки, де дата дорівнює [поточній даті - 1 день]
    k_df = k_df[k_df['Дата'] == (cur_date - timedelta(days=1)).date()]

    # Додамо до result_df датафрейми p_df і k_df (методом concat)
    result_df = pd.concat([result_df, p_df, k_df], ignore_index=True)
    # Зробимо паузу від 1 до 2 секунд
    time.sleep(random.randint(1, 2))
    print(cur_date.strftime('%d.%m.%Y'))

# Додамо стовбчик "Торгова зона" із значенням "ОЕС України (синхронізована з ENTSO-E)"
result_df['Торгова зона'] = 'ОЕС України (синхронізована з ENTSO-E)'
result_df = result_df.replace('< 0,01', 0)  # Коли трапляється значення "< 0,01" замінюємо його на 0
# Деякі стовбчики з числовими значеннями мають str формат. Перетворимо їх на float
result_df['Обсяг заявленої'] = result_df['Обсяг заявленої'].apply(lambda x: float(str(x)))
result_df['Частка заявленої'] = result_df['Частка заявленої'].apply(lambda x: float(str(x)))
result_df['Обсяг акцептованої'] = result_df['Обсяг акцептованої'].apply(lambda x: float(str(x)))
result_df['Частка акцептованої'] = result_df['Частка акцептованої'].apply(lambda x: float(str(x)))
result_df['Частка'] = result_df['Частка'].apply(lambda x: float(str(x)))
result_df['Середньозважена ціна'] = result_df['Середньозважена ціна'].apply(lambda x: float(str(x)))
# Змінимо послідовність на таку:
result_df = result_df[['Дата', 'Торгова зона', 'Суб\'єкт', 'Вид діяльності', 'Обсяг заявленої', 'Частка заявленої',
                       'Обсяг акцептованої', 'Частка акцептованої', 'Частка', 'Середньозважена ціна']]
# Замінимо в result_df
result_df['Суб\'єкт'] = result_df['Суб\'єкт'].replace('Трейдер', 'Трейдери').replace('Постачальник',
                                                                                     'Постачальники').replace(
    'Споживач', 'Споживачі').replace('Виробник електроенергії', 'Виробники')
# З стовбчика "Дата" знайдемо мінімальне значення
date_start = result_df['Дата'].min()
# З стовбчика "Дата" знайдемо максимальне значення
date_end = result_df['Дата'].max()
# Збережемо результати в файл
result_df.to_excel(f'{date_end.strftime("%Y_%m_%d")}_RDN_prodazh_kupivlya.xlsx', index=False)
