"""
URL configuration for daangn project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.urls import path
from category import views

urlpatterns = [
    path('region/', views.user_type_select, name='user_type_select'),
    path('region/<str:user_type>/', views.region_select, name='region_select'),
    path('region/<str:user_type>/<str:TOWN>/', views.region_detail_view, name='region_detail_view'),
]

