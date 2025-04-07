from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from datetime import datetime
import time
import win32com.client as win32
from dateutil.relativedelta import relativedelta
import glob
from tqdm import tqdm
from unipath import Path
import os


#현재 파일의 상위 폴더 위치
PARENT_DIR = os.path.dirname(os.getcwd())
#xls 파일 저장 위치
XLS_FOLDER = os.path.join(PARENT_DIR,'xls_files')
#xlsx 파일 저장 위치
XLSX_FOLDER = os.path.join(PARENT_DIR,'xlsx_files')
# CSV FILE
CSV_FOLDER = os.path.join(PARENT_DIR,'data')

os.makedirs(XLS_FOLDER,exist_ok=True) #xls파일 저장 폴더 생성
os.makedirs(XLSX_FOLDER,exist_ok=True) #xlsx파일 저장 폴더 생성
os.makedirs(CSV_FOLDER,exist_ok=True) #csv파일 저장 폴더 생성




def raw_data_scraper(url):
    """
    KOBIS에서 일별 데이터 파일 다운로드 자동화
    url(str): KOBIS 일별 박스 오피스 주소
    """
    
    
    # 데이터 수집 범위 설정
    start_date = datetime(2024, 12, 17)
    end_date = start_date - relativedelta(years=5)

    # Chrome 옵션 설정
    chrome_options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory":XLS_FOLDER,     # 파일저장 위치
        "download.prompt_for_download": False,       # 다운로드 시 확인 창 비활성화
        "safebrowsing.enabled": True                 # 안전 브라우징 활성화
    }
    chrome_options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=chrome_options)
    driver.maximize_window()

    # 웹페이지 열기
    driver.get(url)

    # 날짜별로 데이터를 수집
    while start_date >= end_date:
        date_str = start_date.strftime("%Y-%m-%d")
        print(f"Processing date: {date_str}")

        # 날짜 선택 (년, 월, 일을 클릭)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'btn_cal')))
        calendar_button = driver.find_element(By.CLASS_NAME, 'btn_cal')
        calendar_button.click()

        # 연도 선택
        year = start_date.year
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'ui-datepicker-year')))
        year_select = driver.find_element(By.CLASS_NAME, 'ui-datepicker-year')
        year_select.click()
        driver.find_element(By.XPATH, f"//option[text()='{year}']").click()

        # 월 선택
        month = start_date.month - 1  # 0부터 시작하는 인덱스
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'ui-datepicker-month')))
        month_select = driver.find_element(By.CLASS_NAME, 'ui-datepicker-month')
        month_select.click()
        driver.find_element(By.XPATH, f"//option[@value='{month}']").click()

        # 일 선택
        day = start_date.day
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.LINK_TEXT, str(day))))
        day_button = driver.find_element(By.LINK_TEXT, str(day))
        day_button.click()

        # 조회 버튼 클릭
        search_button = driver.find_element(By.CLASS_NAME, 'btn_blue')
        search_button.click()

        #엑셀 파일 다운로드 버튼 클릭
        excel_download_xpath = '//*[@id="content"]/div[3]/div/a'
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, excel_download_xpath)))
        excel_download_button = driver.find_element(By.XPATH,excel_download_xpath)
        excel_download_button.click()
        # 팝업 창 확인 버튼 클릭
        alert = driver.switch_to.alert
        alert.accept()
        #다음 주 데이터 수집 전에 2초 Cooloff
        time.sleep(2)

        #새로운 start_date 계산
        start_date = start_date - relativedelta(days=7)

def file_converter():
    """ 
    .xls에서 xlsx로 변경하는 코드
    """
    #xls 파일 리스트 생성
    xls_list = glob.glob(os.path.join(XLS_FOLDER,'*.xls'))

    for xls in tqdm(xls_list):
        # Excel 객체 생성
        excel_app = win32.gencache.EnsureDispatch('Excel.Application')

        # .xls 파일 열기
        wb = excel_app.Workbooks.Open(xls)

        # .xlsx 파일을 다른 위치에 저장하기
        cur_path = Path(xls) # xls 경로 추출
        file_name = cur_path.name # 경로에서 파일명 및 확장자 추출
        new_file_name = str(file_name) + 'x' # 'aaa.xls'에서 'aaa.xlsx'로 변경
        wb.SaveAs(os.path.join(XLSX_FOLDER,new_file_name), FileFormat=51) #FileFormat 51은 엑셀임

        # 파일 닫기
        wb.Close()

        # Excel 종료
        excel_app.Application.Quit()
        pass

def data_extract_and_save():
    """
    XLSX에서 일짜별로 추출 및 장르 데이터 csv로 단일 파일 저장
    """

    # 저장할 테이블의 영문 컬럼명
    new_cols = ['voting_date','ranking','title','released_date','sales',
            'sales_market_portion','sales_comp_yesterday','sales_fluctuation_rate',
            'total_sales','audience_num','audience_num_delta',
            'audience_num_fluctuatation_rate','total_audience','screen_num',
            'total_played_time','country_of_origin','country','production_comp_name',
            'distributor_name','film_ratings','genre','director','performers']
    
    #엑셀에 있는 데이터를 결합하기 위한 빈 DataFrame
    df_combined = pd.DataFrame()
    excel_files = glob.glob(os.path.join(XLSX_FOLDER,'*.xlsx')) #사용할 xlsx 파일 리스트 추출

    for file in tqdm(excel_files):
        df = pd.read_excel(file, skiprows=4, engine="openpyxl")
        #'순위'가 포함된 행(index) 저장
        rank_rows = df[df.apply(lambda row: row.astype(str).str.contains('순위').any(), axis=1)].index
        #'합계'가 포함된 행(index) 저장
        grand_total_rows = df[df.apply(lambda row: row.astype(str).str.contains('합계').any(), axis=1)].index
        
        #엑셀파일에서서 일별 데이터 추출 및 저장 (CSV으로 저장)
        for i, rank_row in enumerate(rank_rows):

            data = df.iloc[rank_row+2:grand_total_rows[i],:].values #해당 날짜의 데이터 추출
            df_daily = pd.DataFrame(data) #excel에 추출한 데이터 DataFrame 생생성
            
            date = df.iloc[rank_row-1].iat[0][1:-3]# 날짜 정보만 추출
            dates = date.split()
            date_str = ''
            for date in dates:
                date_str += date[:-1]#년,월,일 제거후 YYYYMMDD형식으로 변경        
            df_vote_date = pd.DataFrame({'voting_date':[date_str]*len(data)})#해당일자 column 생성
            df_vote_date['voting_date'] = pd.to_datetime(df_vote_date['voting_date'], format="%Y%M%d")# string에서 datetime으로 변경
            df_vote_date['voting_date'] = df_vote_date['voting_date'].apply(lambda x: x.strftime("%Y-%M-%d"))# %Y-%M-%d으로 변경
            df_final = pd.concat([df_vote_date,df_daily],axis=1)# 조회일자와 합치기

            df_final[1] = df_final[1].apply(lambda x: str(x) if isinstance(x,(int,float)) else x) #영화명 (컬럼 1번)이 숫자로만 이루어져 있을 경우 string으로 전환
            df_combined  = pd.concat([df_combined,df_final])
        
    df_combined.columns = new_cols # 컬럼명 변경
    df_combined.to_csv(os.path.join(CSV_FOLDER,'combined_data.csv'),header=True,index=False,encoding='utf-8-sig')#추출한 날짜를 파일명으로 하고 한글깨짐 방지를 위해 utf-8-sig로 encoding후 CSV로 저장

    # 쉼표로 구분된 문자열을 분리하고 explode로 1대1 매핑
    df_combined["genre"] = df_combined["genre"].str.split(",")  # 쉼표 기준으로 리스트로 변환
    df_genre = df_combined.explode("genre")  # 리스트의 각 요소를 행으로 확장

    # 필요시 공백 제거
    df_genre["genre"] = df_genre["genre"].str.strip()

    #Null 값 제거
    df_genre.dropna(inplace=True)

    df_genre.to_csv(os.path.join(CSV_FOLDER,'daily_genre_data.csv'),header=True,index=False,encoding='utf-8-sig')

if __name__ == '__main__':
    # KOBIS 일별 박스오피스 URL
    BASE_URL = "https://www.kobis.or.kr/kobis/business/stat/boxs/findDailyBoxOfficeList.do"
    raw_data_scraper(BASE_URL)
    file_converter()
    data_extract_and_save()

