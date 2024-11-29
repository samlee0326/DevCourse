from django.db import models
import datetime
from django.utils import timezone
from django.contrib import admin

class Question(models.Model):
    question_text = models.CharField(max_length=200,verbose_name='질문')
    pub_date = models.DateTimeField(auto_now_add=True,verbose_name='생성일')
    owner = models.ForeignKey('auth.User', related_name='questions', on_delete=models.CASCADE, null=True)
    
    @admin.display(boolean=True, description='최근생성(하루기준)')
    def was_published_recently(self):
        return self.pub_date >= timezone.now() - datetime.timedelta(days=1)
    def __str__(self):
        return f'제목: {self.question_text}, 날짜: {self.pub_date}'
    
class Choice(models.Model):
    question = models.ForeignKey(Question, on_delete=models.CASCADE)
    choice_text = models.CharField(max_length=200)
    votes = models.IntegerField(default=0)
    
    def __str__(self):
        return f'{self.choice_text}'