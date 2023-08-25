# Датасет "Індекси РДН та середньозважені ціни"
import pandas as pd
from datetime import datetime
from datetime import timedelta
import re
import time
import urllib.request
import xlrd
from dateutil.relativedelta import relativedelta

# Створимо порожній DataFrame
df_res = pd.DataFrame()
date_end = datetime.today()  # Сьогоднішня дата
n = input("Вкажіть, за скільки місяців треба отримати дані: ")
# Знаючи за скільки місяців потрібні дані, можемо знайти початкову дату (date_start)
date_start = date_end - timedelta(days=30 * int(n))
# Починаємо збирати дані.
# Проходимо від date_start до date_end з кроком 1 місяць
cur_date = date_start
while cur_date <= date_end:
    cur_date += relativedelta(months=1)
    try:
        # Завантажуємо дані за поточну дату
        f = urllib.request.urlopen(
            f'https://www.oree.com.ua/index.php/indexes/downloadfile?date={cur_date.strftime("%m")}.{cur_date.strftime("%Y")}&val=IPS').read()
    except:
        time.sleep(4)
        f = urllib.request.urlopen(
            f'https://www.oree.com.ua/index.php/indexes/downloadfile?date={cur_date.strftime("%m")}.{cur_date.strftime("%Y")}&val=IPS').read()
    workbook = xlrd.open_workbook_xls(file_contents=f, ignore_workbook_corruption=True)
    df = pd.read_excel(workbook)
    # В колонках 'Ціна, грн/МВт.год', 'Обсяг продажу, МВт.год', 'Обсяг купівлі, МВт.год', 'Заявлений обсяг продажу, МВт.год', 'Заявлений обсяг купівлі, МВт.год' замінимо кому на крапку, вилалимо ' ' і переконвертуємо значення в float
    df['Base, грн/МВт.год'] = df['Base, грн/МВт.год'].apply(
        lambda x: float(re.sub(' ', '', str(x).replace(',', '.'))))
    df['Peak, грн/МВт.год'] = df['Peak, грн/МВт.год'].apply(
        lambda x: float(re.sub(' ', '', str(x).replace(',', '.'))))
    df['OffPeak, грн/МВт.год'] = df['OffPeak, грн/МВт.год'].apply(
        lambda x: float(re.sub(' ', '', str(x).replace(',', '.'))))
    df['Мінімальна ціна, грн/МВт.год'] = df['Мінімальна ціна, грн/МВт.год'].apply(
        lambda x: float(re.sub(' ', '', str(x).replace(',', '.'))))
    df['Максимальна ціна, грн/МВт.год'] = df['Максимальна ціна, грн/МВт.год'].apply(
        lambda x: float(re.sub(' ', '', str(x).replace(',', '.'))))
    df['Середньозважена ціна, грн/МВт.год'] = df['Середньозважена ціна, грн/МВт.год'].apply(
        lambda x: float(re.sub(' ', '', str(x).replace(',', '.'))))
    # Додамо колонку "Торгова зона" вона має значення "ОЕС України (синхронізована з ENTSO-E)"
    df['Торгова зона'] = 'ОЕС України (синхронізована з ENTSO-E)'
    df['Дата'] = pd.to_datetime(df['Дата'], format='%d.%m.%Y').dt.date
    # Додамо df до df_res
    df_res = pd.concat([df_res, df])
    print(
        f'https://www.oree.com.ua/index.php/indexes/downloadfile?date={cur_date.strftime("%m")}.{cur_date.strftime("%Y")}&val=IPS')
# розмістимо колонки в потрібному порядку
df_res = df_res[['Дата', 'Торгова зона', 'Base, грн/МВт.год', 'Peak, грн/МВт.год', 'OffPeak, грн/МВт.год',
                 'Мінімальна ціна, грн/МВт.год', 'Максимальна ціна, грн/МВт.год',
                 'Середньозважена ціна, грн/МВт.год']]
# З стовбчика "Дата" знайдемо мінімальне значення
date_start = df_res['Дата'].min()
# З стовбчика "Дата" знайдемо максимальне значення
date_end = df_res['Дата'].max()
# Збережемо результати в файл
df_res.to_excel(f'{date_end.strftime("%Y_%m_%d")}_indeksy_RDN_ta_serednozvazheni_tsiny.xlsx', index=False)
