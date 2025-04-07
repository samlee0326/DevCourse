from django.contrib import admin
from .models import Region, ItemCategory, Item, ItemKeyword

admin.site.register(Region)
admin.site.register(ItemCategory)
admin.site.register(Item)
admin.site.register(ItemKeyword)