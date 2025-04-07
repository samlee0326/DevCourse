from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver import ActionChains
import pandas as pd
from datetime import datetime, timedelta
import time
from urllib.parse import unquote
from collections import Counter,OrderedDict

def daangn_crawl():
    """
    Selenium을 이용한 당근 마켓에서 필요한 데이터 수집. 
    """
    categories = [2,8] #취미/게임/음반 , 가구 인테리어 당근마켓 내부에서 사용하는 카테고리 번호
    category_names = ["취미/게임/음반","가구/인테리어"]

    # 각 물건의 포스트 사이트, 포스팅된 시간, 채팅 수, 관심 수 ,조회 수 크롤링
    item_post = [] #선택한 카테고리의 제품의 세부 페이지 웹사이트 저장
    posted_time = [] # 해당 물품이 개시된 날짜
    item_name = [] # 물건 이름을
    item_location = [] # 물건 판매위치
    item_price = [] # 가격
    chat = []#해상 포스트 채팅 빈도수
    interest = []# 해당 포스트 관심수
    views = [] # 해당 포스트 조회수
    cat_names = [] #카데고리 명 저장

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.maximize_window()

    for category,cat_name in zip(categories,category_names):
        target_website = f'https://www.daangn.com/kr/buy-sell/?category_id={category}&in=%EA%B0%95%EB%82%A8%EA%B5%AC-381'
        driver.get(target_website)

        #페이지 2/3 지점으로 스크롤
        scroll_height = driver.execute_script("return document.body.scrollHeight")
        scroll_position = scroll_height * (2/3)
        driver.implicitly_wait(2) # 당근서버로 부터 '더보기' 버튼 나오기까지 최대 2초 대기
        ActionChains(driver).scroll_by_amount(0,scroll_height).perform()

        #'더보기' 버튼 누르기
        button_xpath = '//*[@id="main-content"]/div[1]/div/section/div/div[3]/button'
        driver.implicitly_wait(2).until(EC.presence_of_element_located((By.XPATH,button_xpath))) # 당근서버로 부터 '더보기'버튼의 XPATH거 보일때까지 최대 2초 대기
        button = driver.find_element(By.XPATH,button_xpath)
        ActionChains(driver).click(button).perform()


        #각 포스트 주소 Scraping
        a_tags = driver.find_elements('tag name', 'a')
        time.sleep(1)
        for tag in a_tags:
            data_gtm = tag.get_attribute('data-gtm')  
            if data_gtm == 'search_article':  
                item_webpage = tag.get_attribute('href')
                item_post.append(item_webpage)
                cat_names.append(cat_name)

    #각 포스트 주소로 이동해서 데이터 수집 
    for idx,item in enumerate(item_post):
        try:
            driver.get(item)

            #당근 마켓에 올려진 시간 XPATH를 통해 구하기
            time_xpath = '//*[@id="main-content"]/article/div[1]/div[2]/section[2]/div[1]/h2/time'
            driver.implicitly_wait(2).until(EC.presence_of_element_located(By.XPATH,time_xpath)) # 당근서버로 부터 아이템 포스트가 완벽하게 구현될때까지 최대 2초 대기
            
            post_time = driver.find_element(By.XPATH,time_xpath)
            datetime_attr = post_time.get_attribute('datetime')
            posted_time.append(datetime_attr[:-6])

            #포스팅 제목 XPATH를 통해 추출
            item_name_xpath = '//*[@id="main-content"]/article/div[1]/div[2]/section[2]/div[1]/div/h1'


            item_name.append(driver.find_element(By.XPATH,item_name_xpath).text)

            #가격 정보 XPATH를 통해 추출
            item_price_xpath = '//*[@id="main-content"]/article/div[1]/div[2]/section[2]/div[1]/h3'
            item_price.append(driver.find_element(By.XPATH,item_price_xpath).text)
            
            
            #위치 정보 XPATH를 통해 추출
            item_location_xpath = '//*[@id="main-content"]/article/div[1]/div[2]/section[1]/div[2]/div/div/div[1]/div/a[2]'
            item_location.append(driver.find_element(By.XPATH,item_location_xpath).text)

            #채팅 수, 관심수, 조회수 XPATH를 통해 추출
            for i in range(1,4):
                num_xpath = f'//*[@id="main-content"]/article/div[1]/div[2]/section[2]/div[2]/span[{i}]'
                val = driver.find_element(By.XPATH,num_xpath).text.split()
                if val[0] == '채팅':
                    chat.append(int(val[-1]))
                elif val[0] == '관심':
                    interest.append(int(val[-1]))
                else:
                    views.append(int(val[-1]))
            print(f'{idx}th item crawled')
        except:
            print(f'Crawling {idx}th item failed')
            continue
        time.sleep(0.5) # 당근마켓 서버에 부하를 줄이기 위해 0.5초 대기

    #위에서 추출한 데이터 Dictionary로 취합
    item_dict = {'category':cat_names,
                'item_name':item_name,
                'region':item_location,
                'district':['강남구']*len(item_name),
                'price':item_price,
                'original_url':item_post,
                'chat_count':chat,
                'interest_count':interest,
                'view_count':views,
                'posted_at':posted_time,
                }

    # 향후 데이터 처리를 용이하게 할수 있도록 Pandas 데이터 프레임으로 저장 
    unprocessed_df = pd.DataFrame(item_dict)
    return unprocessed_df

if __name__ == '__main__':
     daangn_crawl()