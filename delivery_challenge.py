import argparse
import logging
import requests
import time
import datetime as dt
import json

"""
Skeleton for Squirro Delivery Hiring Coding Challenge
August 2021
"""

log = logging.getLogger(__name__)


class NYTimesSource(object):
    """
    A data loader plugin for the NY Times API.
    """
    day_limit = 4000

    def __init__(self):
        self.page = 0
        self.call_count = 0
        self.record_queue = []
        self.total_records = None

    def connect(self, inc_column=None, max_inc_value=None):
        log.debug("Incremental Column: %r", inc_column)
        log.debug("Incremental Last Value: %r", max_inc_value)

    def disconnect(self):
        """Disconnect from the source."""
        # Nothing to do
        pass

    def getDataBatch(self, batch_size):
        """
        Generator - Get data from source on batches.

        :returns One list for each batch. Each of those is a list of
                 dictionaries with the defined rows.
        API LIMITS:
            4,000 requests per day
            10 requests per minute
            wait 6 sec between each request
        """
        batch_list = []
        # Step one: send api key and search for keyword
        # MAKE INITIAL REQUEST TO API
        self.requestArticles(self.page)
        # keep creating batches for all 'hits' found for keyword query
        while self.total_records > 0:
            # first make sure Queue has enough to make batch
            while batch_size > len(self.record_queue):
                # make another api call to add more to queue
                self.page += 1
                self.requestArticles(self.page)
            # create batches according to batch_size
            while len(batch_list) < batch_size:
                # pop and flatten record from queue into batch
                flattened_article = self.flattenData(self.record_queue.pop(0))
                batch_list.append(flattened_article)

            if len(batch_list) >= batch_size:
                yield batch_list
                batch_list = []

        if self.total_records == 0:
            print("task completed")

    def flattenData(self, dat_dict, sep="."):
        flat_dict = {}

        # call to see which data type is inside the next
        # level of the dict and call recursively until
        # we reach a type that is not list or dict
        def recursiveLogic(inner_element, parent_key=''):
            if isinstance(inner_element, list):
                for i in range(len(inner_element)):
                    recursiveLogic(inner_element[i], parent_key + sep + str(i) if parent_key else str(i))
            elif isinstance(inner_element, dict):
                for k, v in inner_element.items():
                    recursiveLogic(v, parent_key + sep + k if parent_key else k)
            else:
                # there was no more nested objects so we store the key
                flat_dict[parent_key] = inner_element

        recursiveLogic(dat_dict)
        return flat_dict

    def requestArticles(self, pageNum):
        """
        Will assign instance vars:
            - record_queue
            - call_count
            - total_records
        Will only call API if the call_count is < day_limit
            - will decrease total record amount for each api call
        Otherwise, waits 24 hours.
        """
        base_url = 'https://api.nytimes.com/svc/search/v2/articlesearch.json'
        requestHeaders = {
            "Accept": "application/json"
        }
        payload = {'page': pageNum, 'keyword': config['query'], 'api-key': config['api_key']}

        if self.call_count <= self.day_limit:
            time.sleep(6)
            r = requests.get(base_url, params=payload, headers=requestHeaders)
            response = r.json()
            # Assign total_records with 'hits' from API
            if self.total_records is None:
                self.total_records = response.get('response', {}).get('meta', {}).get('hits', None)
            # reduce the total_records by the number of returned records
            self.total_records = self.total_records - len(response.get('response', {}).get('docs', None))

            # track call count for day limit then add to record queue
            self.call_count += 1
            # response['docs'] from api is a list of records (dicts)
            self.record_queue.extend(response.get('response', {}).get('docs', None))

        else:
            print("limit reached, waiting for 24 hours")
            self.waitForTomorrow()
            self.call_count = 0

    def getSchema(self):
        """
        Return the schema of the dataset
        :returns a List containing the names of the columns retrieved from the
        source
        """
        if len(self.record_queue) == 0:
            self.requestArticles(0)

        flat_d = self.flattenData(self.record_queue[0])
        schema = [i for i in flat_d.keys()]
        return schema

    def waitForTomorrow(self):
        tomorrow = dt.datetime.replace(dt.datetime.now() + dt.timedelta(days=1), hour=0, minute=0, second=0)
        delta = tomorrow - dt.datetime.now()
        time.sleep(delta.seconds)


if __name__ == "__main__":
    config = {
        "api_key": "s2LADyh59UVUmc8R0xuGVlTJKtAGzeQ6",
        "query": "Silicon Valley",
    }
    source = NYTimesSource()

    # This looks like an argparse dependency - but the Namespace class is just
    # a simple way to create an object holding attributes.
    source.args = argparse.Namespace(**config)

    for idx, batch in enumerate(source.getDataBatch(20)):
        print(f"{idx} Batch of {len(batch)} items")
        for item in batch:
            print(f"  - {item['_id']} - {item['headline.main']}")

