from django.urls import path
from .views import WeatherForecastView

urlpatterns = [
    path('weather/', WeatherForecastView.as_view(), name='weather_forecast'),
]
