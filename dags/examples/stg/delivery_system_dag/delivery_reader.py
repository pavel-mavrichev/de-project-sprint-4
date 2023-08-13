from datetime import datetime
from typing import Dict, List
import requests
import json


class DeliveryReader:
    def __init__(self, delivery_api_endpoint: str, headers: Dict, log) -> None:
        self.delivery_api_endpoint = delivery_api_endpoint
        self.headers = headers
        self.method_url = '/deliveries'
        self.log = log

    def get_deliveries(self, load_threshold: datetime) -> List[Dict]:

        params = {
            'from': load_threshold.strftime("%Y-%m-%d %H:%M:%S"),
            'sort_field': 'id', 
            'sort_direction': 'asc',
            'offset': 0
            }

        result_list = []

        r = requests.get(self.delivery_api_endpoint + self.method_url, headers=self.headers, params=params)
        while r.text != '[]':
            self.log.info(str(r.content))
            self.log.info(str(r.url))
            response_list = json.loads(r.content)

            for delivery_dict in response_list:
                result_list.append({'object_id': delivery_dict['delivery_id'], 
                                    'object_value': delivery_dict,
                                    'update_ts': datetime.fromisoformat(delivery_dict['delivery_ts'])})
                
            params['offset'] += len(response_list)

            r = requests.get(self.delivery_api_endpoint + self.method_url, headers=self.headers, params=params)

        return result_list
