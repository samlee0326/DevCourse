from .data_scraper import daangn_crawl
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
import time
from konlpy.tag import Hannanum
from dateutil.relativedelta import relativedelta
from urllib.parse import unquote
from collections import Counter,OrderedDict
from .models import Region,ItemCategory,Item,ItemKeyword
from django.utils.timezone import make_aware

def decoder(x):
    """
    상세 페이지 뒷부분 변경(decoding)하는 함수

    Parameter:
    - x (str): 문자열로 인코딩된 값

    Returns:
    - converted (str): x를 디코딩한 값
    """

    decode = unquote(x)

    return decode

def price_converter(val):
    """
    문자열에서 '원' 제거 및 콤마 제거 후 숫자만 리턴

    Parameter: 
    - val (str): 물건 값
    
    Returns:
    price(int): integer로 변환된 값     
    """
    price = 0
    if '원' in val:
        price = int(val[:-1].replace(',', ''))
    else:
        price= 0

    return price
    

def district_to_num(x):
    """
    행정명을 숫자(id)로 변경

    Parameter
    - x (str): 행정명

    Returns
    - id(int): 행정명에 해당되는 id값
    """
    id = 999
    if x in district_dict[x]:
        id = district_dict[x]
    else:
        pass
    return id
    
def category_to_num(x):
    """
    카테고리명(str)을 id(int)로 변경
    
    Parameter
    - x(str): 카테고리명
    
    Returns
    - id(int): 카테고리에 해당되는 id값
    """
    id = 999
    if x in category_dict:
        id = category_dict[x]
    else:
        pass
    return id

def df_processor(df):
    """
    DataFrame 데이터 전처리 후 저장

    Parameters:
    - df(pandas DataFrame) - 수집된 데이터로 이루어진 DataFrame

    Returns:
    - processed_df(pandas DataFrame) - 처리된 DataFrame

    """
    df['posted_at'] = pd.to_datetime(df['posted_at']).dt.floor('S') #str -> datetime으로 변경
    df['price'] = df['price'].apply(price_converter) # 가격 프로세싱 str에서 int로 변경
    df['region_id'] = df['region'].apply(district_to_num) #행정동에서 id로 mapping
    df['category_id'] = df['category'].apply(category_to_num) #카테고리에서 id로 mapping
    df['detail_url'] = df['original_url'].apply(decoder)# url 끝단 decoding 하기
    
    processed_df = df[df['region_id'] != 999]#강남구 외 지역 제거
    return processed_df


def region_table_creator(df):
    """
    Django 모델의 Region 테이블을 위한 데이터 생성 및 데이터 삽입
    
    Parameter:
    -df (Pandas DataFrame): 함수 df_processor에서 리턴된 Pandas DataFrame

    Returns:
    - None

    """
    region_df = df[['region_id','district','region']]

    for _, row in region_df.iterrows():
        Region.objects.get_or_create(
            id = int(row["id"]),
            district = row["district"],
            town = row["region"],
            created_at = make_aware(datetime.now().replace(microsecond=0)),
            updated_at = make_aware(datetime.now().replace(microsecond=0))

        )


def itemcategory_table_creator():
    """
    Django 모델의 ItemCategory 테이블을 위한 데이터 생성 및 데이터 삽입
    
    Parameter:
    - None

    Returns:
    - None

    """
    df = pd.read.csv('data/category_data.csv')
    for _, row in df.iterrows():
        ItemCategory.objects.get_or_create(
            id = int(row["id"]),
            name = row["name"],
            created_at = make_aware(datetime.now().replace(microsecond=0)),
            updated_at = make_aware(datetime.now().replace(microsecond=0))
            )
    

def item_table_creator(df):
    """
    Django 모델의 Item 테이블을 위한 데이터 생성 및 데이터 삽입
    
    Parameter:
    -df (Pandas DataFrame): 전처리된 Pandas DataFrame

    Returns:
    - None

    """
    item_df = df[['category_id','region_id',
                'item_name','price','detail_url',
                'chat_count','interest_count','view_count',
                'posted_at']] 
    
    for _, row in item_df.iterrows():
        Item.objects.get_or_create(
            category_id = int(row["category_id"]),
            region_id = int(row["region_id"]),
            name = row["name"],
            price = row["price"],
            detail_url = row["detail_url"],
            chat_count = int(row["chat_count"]),
            interest_count = int(row["interest_count"]),
            view_count = int(row["view_count"]),
            posted_at = make_aware(datetime.strptime(row["posted_at"],"%Y-%m-%d %H:%M")),
            created_at =  make_aware(datetime.now().replace(microsecond=0)),
            updated_at =  make_aware(datetime.now().replace(microsecond=0))
            )
        

def itemkeyword_table_creator(df):
    """
    Django 모델의 Itemkeyword 테이블을 위한 데이터 생성 및 데이터 삽입
    
    Parameter:
    -df (Pandas DataFrame): 전처리된 Pandas DataFrame

    Returns:
    - None

    """
    item_keyword_df = df[['region_id','category_id','item_name']] 

    #Region 별로 Item_name 묶기
    grouped = item_keyword_df.groupby('region_id')['item_name'].apply(' '.join).reset_index()
    hannanum = Hannanum()

    def extract_noun_frequencies(text):
        nouns = hannanum.nouns(text)  # 텍스트에서 명사 추출
        return Counter(nouns)  # 명사의 빈도수 계산

    # region_id별로 명사 빈도수 매핑
    grouped['frequencies'] = grouped['item_name'].apply(extract_noun_frequencies)
    result = []

    for _, row in grouped.iterrows():
        region_id = row['region_id']
        for noun, freq in row['frequencies'].items():
            result.append({'region_id': region_id, 'name': noun, 'frequency': freq})

    # 결과 DataFrame
    final_item_df = pd.DataFrame(result)

    for _, row in final_item_df.iterrows():
        region_instance = Region.objects.get(id=int(row['region_id']))

        ItemKeyword.objects.get_or_create(
            name = row["name"],
            region = region_instance,
            frequency = int(row["frequency"]),
            created_at = make_aware(datetime.now().replace(microsecond=0)),
            updated_at = make_aware(datetime.now().replace(microsecond=0))
        )

if __name__ == '__main__':
    data_scraper = daangn_crawl()
    
    region_df = pd.read_csv('data/region_data.csv')
    district_dict = dict(zip(region_df.town,region_df.id))
    
    category_df = pd.read_csv('data/category_data.csv')
    category_dict = dict(zip(category_df.name,category_df.id))

    processed_data = df_processor(data_scraper)
    processed_data.to_csv('data/processed_data.csv',header=True, index=False,encoding = 'utf-8-sig')

    #Django의 Region Table에 데이터 삽입
    region_table_creator(processed_data)

    #Django에 item_category table 데이터 삽입
    itemcategory_table_creator()

    #Django에 item 테이블 데이터 삽입
    item_table_creator(processed_data)

    #Django에 ItemKeyword 테이블 데이터 삽입
    itemkeyword_table_creator(processed_data)

