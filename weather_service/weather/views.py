import logging
import requests
from django.http import JsonResponse
from django.views import View
from .services.weather_service import WeatherService

logger = logging.getLogger(__name__)

class WeatherForecastView(View):
    def get(self, request):
        city_name = request.GET.get('city')
        if not city_name:
            return self.error_response('City name is required', status=400)

        try:
            result = WeatherService.get_city_and_weather_info(city_name=city_name)
            
            if not result:
                return self.error_response('City not found', status=404)

            return JsonResponse(result, safe=False, status=200)
        
        except requests.HTTPError as http_err:
            logger.error(f'HTTP error occurred for city "{city_name}": {http_err}')
            return self.error_response('An error occurred, please try again later.', status=500)
        
        except requests.RequestException as req_err:
            logger.error(f'Request error occurred for city "{city_name}": {req_err}')
            return self.error_response('An error occurred, please try again later.', status=500)
        
        except Exception as err:
            logger.error(f'An unexpected error occurred for city "{city_name}": {err}')
            return self.error_response('An error occurred, please try again later.', status=500)
    
    def error_response(self, message: str, status: int) -> JsonResponse:
        """
        Helper method to return an error JsonResponse.

        Args:
            message (str): Error message to be returned.
            status (int): HTTP status code for the response.

        Returns:
            JsonResponse: Formatted error response.
        """
        return JsonResponse({'error': message}, status=status)
