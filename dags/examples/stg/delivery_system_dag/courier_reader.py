from datetime import datetime
from typing import Dict, List
import requests
import json


class CourierReader:
    def __init__(self, delivery_api_endpoint: str, headers: Dict) -> None:
        self.delivery_api_endpoint = delivery_api_endpoint
        self.headers = headers
        self.method_url = '/couriers'

    def get_couriers(self) -> List[Dict]:

        params = {
            'sort_field': 'id', 
            'sort_direction': 'asc',
            'offset': 0
            }

        result_list = []

        r = requests.get(self.delivery_api_endpoint + self.method_url, headers=self.headers, params=params)
        while r.text != '[]':
            response_list = json.loads(r.content)

            for courier_dict in response_list:
                result_list.append({'object_id': courier_dict['_id'], 
                                    'object_value': courier_dict,
                                    'update_ts': datetime.now()})
                
                params['offset'] += 1

            r = requests.get(self.delivery_api_endpoint + self.method_url, headers=self.headers, params=params)

        return result_list
