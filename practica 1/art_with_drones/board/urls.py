# board/urls.py

from django.urls import path
from .views import board_view
from . import views

urlpatterns = [
    path('', board_view, name='board'),
    path('update_positions/', views.update_positions, name='update_positions'),

]
