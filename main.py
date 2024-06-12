import pandas as pd
import numpy as np  # NumPy'yı import ettik
from sqlalchemy import create_engine #SQL bağlantısı 


def csv_to_sql(csv_file:str, table_name:str, date, database_name:str, not_included:list):

    #SQL bağlantısı
    engine = create_engine(f"mysql+pymysql://remote:BIw883k8@212.31.2.93/{database_name}",
                        connect_args={"charset": "utf8mb4"}, echo=False)

    # CSV dosyasını oku
    df = pd.read_csv(f'{csv_file}', encoding='utf-8')

    # 'n/a' değerlerini NaN ile değiştir (isteğe bağlı)
    df.replace('n/a', pd.NA, inplace=True)

    # "undefined" değerlerini boş string ile değiştir
    df.replace('undefined', '', inplace=True)

    # "author" sütununda NaN değerlerini 'listing' ile değiştir
    df['author'].replace(pd.NA, 'listing', inplace=True)

    # Diğer NaN değerlerini np.nan ile değiştir (SQL için NULL olacaktır)
    df.fillna(np.nan, inplace=True)

    date = pd.to_datetime(date)
    # Yeni sütunu ekleyin ve tüm satırlar için değeri '2023-08-31' yapın
    df['reportdate'] = date

    # "datePublished" sütununda boş değerleri '0000-00-00 00:00:00' ile değiştir
    df['datePublished'].replace(pd.NA, '1970-01-01 00:00:01', inplace=True)
    df['datePublished'].replace('', '1970-01-01 00:00:01', inplace=True)

    # "dateModified" sütununda boş değerleri '0000-00-00 00:00:00' ile değiştir
    df['dateModified'].replace(pd.NA, '0000-00-00 00:00:00', inplace=True)
    df['dateModified'].replace('', '0000-00-00 00:00:00', inplace=True)

    df['avgScrollPercentage'].replace("", None, inplace=True)
    df['avgPageDuration'].replace("", None, inplace=True)

    #istenmeyen sütunların kaldır
    drop_columns = not_included
    for drop_column in drop_columns:
        df.drop(drop_column, axis=1, inplace=True)
    
    print(df)

    # DataFrame'i SQL'e kaydet
    df.to_sql(f'{table_name}', engine, index=False, if_exists="append")
    print(f"{table_name} Güncellendi")    
# İhtiyaçlara göre değiştirilecek kısım
csv_to_sql(csv_file = "kicerikfull.csv", table_name = "kicerikfull", date = "2024-05-31 00:00:00",
            database_name = "editorraporlari2", not_included = ["videoIds", "dateModified", "breadcrumb_list", "section"])



