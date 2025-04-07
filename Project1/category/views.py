from django.db.models import Count, Sum,F
from .models import *
from django.shortcuts import render
from collections import Counter
from konlpy.tag import Hannanum

def user_type_select(request):
    """
    사용자 타입 선택 페이지
    """

    return render(request, 'category/index.html')
    
    
def region_select(request, user_type):
    """
    지역 선택 페이지
    """

    regions = Region.objects.all().order_by('town')
    #region_list = list(regions.values('id', 'town'))
    context = {'regions': regions, 'user_type': user_type}
    #return JsonResponse(context)
    return render(request, 'category/region_select.html', context)
    


def top_category_with_keywords_seller(request, TOWN):
    """
    지역의 최상위 카테고리와 해당 카테고리의 키워드 및 빈도를 반환합니다.

    Parameters:
    TOWN (str): 조회하고자하는 행정명 (예: 도곡동)

    Returns:
    context: HTML에 전달할 입력받은 행정명,상위 카테고리, 키워드를 포함한 데이터 
    """
    #0. 입력받은 동이름 이름의 데이터  
    region = Region.objects.filter(town=TOWN).first()

    # 1. 최빈도  키워드 계산
    top_keyword = (
        ItemKeyword.objects.filter(region=region)
        .values('name')  # 키워드만 추출
        .annotate(total_frequency=F('frequency'))  # 빈도수 추출
        .order_by('-total_frequency')  # 빈도수 내림차순 정렬
        .first()  # 최빈 키워드 가져오기
        )

    # 2. Item 테이블의 해당 카테고리 QuerySet추출 
    matched_items = Item.objects.filter(region=region, name__icontains=top_keyword['name'])

    # 3. 여러 카테고리가 있을 경우 그중 최빈도 카테고리 데이터만 추출
    top_category = (
    matched_items
    .values('category__name')  # 카테고리 이름 추출
    .annotate(item_count=Count('id'))  # 각 카테고리별 아이템 수 계산
    .order_by('-item_count')  # 아이템 수 기준으로 정렬
    .first()  # 최빈 카테고리 가져오기
    )
    
    top_category_items = Item.objects.filter(category__name=top_category['category__name'])
    names = [item.name for item in top_category_items]

    # 4. 추출한 데이터에서 명사만 추출하기
    keywords = []
    hannanum = Hannanum()
    for name in names:
        words = hannanum.nouns(name)
        for word in words:
            keywords.append(word)
    keyword_count = Counter(keywords) #키워드: 빈도로 되어 있는 dictionary
    keywords = sorted(keyword_count.items(), key=lambda x:x[1],reverse=True) #키워드 빈도 내림차순으로 정렬
    if len(keywords)>10:
        keyword_lst = [i for i,_ in keywords][:10] #키워드 10개만 추출
    else:
        keyword_lst = [i for i,_ in keywords]

    
    # 데이터 전달
    context = {
        "town": TOWN,
        "top_category": top_category['category__name'],
        "top_keywords": keyword_lst, 
    }

    #return JsonResponse(context)
    return render(request, 'category/detail_seller.html', context)

def top_category_with_view_count_consumer(request,TOWN):
    """
    사용자가 선택한 지역 포스팅 중에 조회수(view_count) 합이 가장 높은 카테고리와 해당 카테고리의 키워드 (명사)를 반환합니다.

    Parameters:
    TOWN (str): 조회 하고자 하는 행정명 (예: 도곡동)

    Returns:
    context (dict): HTML에 전달할 dictionary 형식의 데이터
    """
    
    # 0. 입력받은 지역명의 id 조회  
    location_id = Region.objects.filter(town=TOWN).first()
    
    # 1. view_count가 최대인 카테고리
    view_count_total_by_category = (
    Item.objects.filter(region=location_id)
    .values('category__name')  # 카테고리별 그룹화
    .annotate(view_count_sum=Sum('view_count'))  # 각 카테고리의 최대 ViewCount 계산
    .order_by('-view_count_sum')# view_count합 내림차순 정렬
    .first()# view_count가 가장 높은 category만 추출
    )

    # 2. Item 테이블에서 포스트 글 추출
    top_category_items = Item.objects.filter(category__name=view_count_total_by_category['category__name'])
    names = [item.name for item in top_category_items]

    # 3. 추출한 포스트에서 명사만 추출하기
    keywords = []
    hannanum = Hannanum()
    for name in names:
        words = hannanum.nouns(name)
        for word in words:
            keywords.append(word)
    keyword_count = Counter(keywords) #키워드 빈도가 있는 dictionary
    keywords = sorted(keyword_count.items(), key=lambda x:x[1],reverse=True) #키워드 빈도 내림차순으로 정렬
    if len(keywords)>10:
        keyword_lst = [i for i,_ in keywords][:10] #키워드 10개만 추출
    else:
        keyword_lst = [i for i,_ in keywords]#키워드가 10개 이하일시 모두 추출

    context = {
    "town": TOWN,
    "top_category": view_count_total_by_category['category__name'],
    "top_keywords": keyword_lst, 
    }
    
    #return JsonResponse(context)
    return render(request, 'category/detail_consumer.html', context)
    


def region_detail_view(request,TOWN,user_type):
    if user_type == 'consumer':
        return top_category_with_view_count_consumer(request,TOWN)
    else:
        return top_category_with_keywords_seller(request,TOWN)
