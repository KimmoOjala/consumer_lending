from django.urls import path
from . import views

urlpatterns = [
    path('start/', views.authorization_request, name='start'),
    path('end/', views.access_token_request, name='end'),
    path('report/', views.report, name='report'),
    #path('json_save/', views.json_save_test, name='json_save'),
    #path('json_load/', views.json_load_test, name='json_load'),
    #path('start_test/', views.authorization_request_test, name='start_test'),
    #path('end_test/', views.access_token_request_test, name='end_test'),
    #path('process/', views.process_test, name='process'),
    #path('response_test/', views.token_request_respose_test, name='response_test')
]

