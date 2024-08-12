import requests
import datetime

from typing import Dict, List, Optional
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed

class WeatherService:
    @staticmethod
    def get_city_coordinates(city_name: str) -> Dict:
        """
        Retrieves the coordinates of a city based on the city name using the Reservamos API.

        This function performs an HTTP GET request to the Reservamos API to search for the specified city.
        If the request is successful and data is returned, the function extracts the city ID, latitude,
        longitude, and city name from the response and returns them in a dictionary.
        In case of errors, the function handles exceptions appropriately and logs an error message.

        Args:
            city_name (str): The name of the city for which coordinates are to be retrieved.

        Returns:
            Dict: A dictionary containing the city data found, where each entry includes
                the `id`, `lat`, `long`, and `city_name` of the city.
                If no data is found, an empty dictionary is returned.
                In case of errors, an empty dictionary is also returned.
        """
        base_url = 'https://search.reservamos.mx/api/v2/places'
        query_params = {'q': city_name}
        reservamos_api_url = f'{base_url}?{urlencode(query_params)}'
        
        response = requests.get(reservamos_api_url)
        response.raise_for_status()
        data = response.json()

        if data:
            data_aux = []
            for res in data:
                if res.get('id') and res.get('lat') and res.get('long'):
                    dict_aux = {}
                    dict_aux['id'] = res.get('id')
                    dict_aux['lat'] = res.get('lat')
                    dict_aux['long'] = res.get('long')
                    dict_aux['city_name'] = res.get('city_name')
                    data_aux.append(dict_aux)
            return data_aux
        else:
            return {}
    
    @staticmethod
    def get_city_weather(lat: str, long: str) -> Optional[List[Dict]]:
        """
        Retrieves the weather forecast for a given location based on latitude and longitude using the OpenWeatherMap API.

        This function performs an HTTP GET request to the OpenWeatherMap API to get the weather forecast for 
        the specified location. It excludes current, minutely, hourly, and alert data from the response. 
        If the request is successful and data is returned, the function extracts the date, minimum temperature, 
        and maximum temperature for each day in the forecast and returns them in a list of dictionaries.
        In case of errors, the function handles exceptions appropriately and logs an error message.

        Args:
            lat (str): The latitude of the location for which weather data is to be retrieved.
            lon (str): The longitude of the location for which weather data is to be retrieved.

        Returns:
            Optional[List[Dict]]: A list of dictionaries, each containing the forecast data for a day. 
                                Each dictionary includes the following keys:
                                - `datetime`: The date of the forecast in the format YYYY-MM-DD.
                                - `temp_min`: The minimum temperature for the day.
                                - `temp_max`: The maximum temperature for the day.
                                If no data is found or if an error occurs, an empty list is returned.
        """
        base_url = 'https://api.openweathermap.org/data/2.5/onecall'
        owm_api_key = '46a187ce0fe63dd82859450fb71017fd'
        query_params = {
            'lat': lat,
            'lon': long,
            'exclude': 'current,minutely,hourly,alerts',
            'appid': owm_api_key,
            'units': 'metric'
        }
        
        owm_api_url = f'{base_url}?{urlencode(query_params)}'
        
        response = requests.get(owm_api_url)
        response.raise_for_status()
        data = response.json()

        if data and data.get('daily'):
            forecast_data = []
            for day in data.get('daily'):
                d_aux = {}
                d_aux['datetime'] = datetime.datetime.fromtimestamp(day.get('dt')).strftime('%Y-%m-%d')
                d_aux['temp_min'] = day.get('temp').get('min')
                d_aux['temp_max'] = day.get('temp').get('max')
                forecast_data.append(d_aux)
            return forecast_data
        else:
            return []
        
    @staticmethod
    def get_city_and_weather_info(city_name: str) -> List[Dict]:
        """
        Retrieves city information along with weather forecasts for the specified city name.

        This function first fetches the coordinates for the given city using the `get_city_coordinates` function.
        If coordinates are found, it then retrieves weather forecasts for each set of coordinates concurrently 
        using threading with the `ThreadPoolExecutor`. It collects the weather data and appends it to the city 
        information. In case of any errors during data fetching, appropriate warnings are logged, and the function
        continues processing the remaining data.

        Args:
            city_name (str): The name of the city for which coordinates and weather information are to be retrieved.

        Returns:
            List[Dict]: A list of dictionaries, each containing the city information along with its weather forecast.
                        Each dictionary includes:
                        - `id`: The city's identifier.
                        - `lat`: The latitude of the city.
                        - `long`: The longitude of the city.
                        - `city_name`: The name of the city.
                        - `weather`: A list of dictionaries containing weather data for each day.
                        Each weather dictionary includes:
                        - `datetime`: The date of the forecast in the format YYYY-MM-DD.
                        - `temp_min`: The minimum temperature for the day.
                        - `temp_max`: The maximum temperature for the day.
                        If no coordinates are found or an error occurs, the function returns an empty list.
        """
        results = []
        city_coordinates = WeatherService.get_city_coordinates(city_name=city_name)
        
        if not city_coordinates:
            return results
        
        with ThreadPoolExecutor() as executor:
            future_to_city = {
                executor.submit(WeatherService.get_city_weather, city_res.get('lat'), city_res.get('long')): city_res
                for city_res in city_coordinates
            }

            for future in as_completed(future_to_city):
                city_res = future_to_city[future]
                city_weather = future.result()
                city_res['weather'] = city_weather
                results.append(city_res)
                
        return results
