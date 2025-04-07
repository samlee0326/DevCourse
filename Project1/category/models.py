from django.db import models

# Region_table
class Region(models.Model):
    id = models.AutoField(primary_key=True)  # 기본키
    district = models.CharField(max_length=50)  # 구
    town = models.CharField(max_length=50)  # 동
    created_at = models.DateTimeField(auto_now_add=True)  # 생성일
    updated_at = models.DateTimeField(auto_now=True)  # 수정일

    def __str__(self):
        return f"{self.district} {self.town}"  # 관리자 페이지 표시용

# ItemCategory_table
class ItemCategory(models.Model):
    id = models.AutoField(primary_key=True)  # 기본키
    name = models.CharField(max_length=50)  # 카테고리 이름
    created_at = models.DateTimeField(auto_now_add=True)  # 생성일
    updated_at = models.DateTimeField(auto_now=True)  # 수정일

    def __str__(self):
        return self.name  # 관리자 페이지 표시용

# Item_table
class Item(models.Model):
    id = models.AutoField(primary_key=True)  # 기본키
    region = models.ForeignKey(Region, on_delete=models.CASCADE)  # 외래키: Region 참조
    category = models.ForeignKey(ItemCategory, on_delete=models.CASCADE)  # 외래키: ItemCategory 참조
    name = models.CharField(max_length=50)  # 상품명
    price = models.IntegerField(default=0)  # 가격
    detail_url = models.CharField(max_length=255)  # 상세 페이지 URL
    chat_count = models.IntegerField(default=0)  # 채팅 수
    interest_count = models.IntegerField(default=0)  # 관심 수
    view_count = models.IntegerField(default=0)  # 조회 수
    posted_at = models.DateTimeField()  # 중고 등록일
    created_at = models.DateTimeField(auto_now_add=True)  # 생성일
    updated_at = models.DateTimeField(auto_now=True)  # 수정일

    def __str__(self):
        return self.name  # 관리자 페이지 표시용

# ItemKeyword_table
class ItemKeyword(models.Model):
    id = models.AutoField(primary_key=True)  # 기본키
    region = models.ForeignKey(Region, on_delete=models.CASCADE, null=True)  # 외래키: Region 참조
    name = models.CharField(max_length=100)  # 키워드
    frequency = models.IntegerField(default=0)  # 빈도수
    created_at = models.DateTimeField(auto_now_add=True)  # 생성일
    updated_at = models.DateTimeField(auto_now=True)  # 수정일

    def __str__(self):
        return self.name  # 관리자 페이지 표시용