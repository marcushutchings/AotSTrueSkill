#!/usr/bin/python3
# Calculate all player's TrueSkill in Ashes of the Singularity: Escalation
# Can only reliably rate players using Ranked and Unranked queue matches
# Other matches don't declare whether AI is used or whether players had a resource bonus

# JSON - load config and read data
# HTTP Headers - for handling next request
# Multi-threading - to quickly process the records
# TrueSkill - to calculate player's skill
# MongoDB- store player documents

import json
import requests

APP_CONFIG_FILE = "trueskill.conf"

class TrueSkillConfig:
    baseurl = "";
    next_record_index = 0;

    def __init__(self, config_file_path):
        with open(config_file_path) as fh:
            data = json.load(fh)

        self.base_url = data['baseurl']
        self.next_record_index = data['nextrecordindex']

        print("Base API URL is", self.base_url)
        print("Next record to get is", self.next_record_index)

class MatchPage:
    page_index = 0
    json_data = [] 

    def __init__(self, page_index, json_data):
        self.page_index = page_index
        self.json_data = json_data

        print(self.json_data)


class MatchDataRepository:
    base_url = ""
    next_match_to_get = 0
    total_matches = 0
    match_pages = []

    def __init__(self, config):
        self.base_url = config.base_url
        self.next_match_to_get = config.next_record_index
        self._init_connection()
        self._load_matches()

    def _init_connection(self):
        with requests.get(self.base_url) as response:
            if response.ok:
                self.total_matches = response.headers['X-Total']
                print("Current number of matches played is", self.total_matches)

    def _load_matches(self):
        if self.total_matches == 0:
            return
        self.match_pages.append(self._load_matches_page(0))

    def _load_matches_page(self, page):
        request_url = self.base_url + "?offset= " + str(page*50)
        print("request is", request_url)
        with requests.get(request_url) as response:
            if response.ok:
                data = json.loads(response.content)
                return MatchPage(0, data)
        return None


def main():
    config = TrueSkillConfig(APP_CONFIG_FILE)
    matches = MatchDataRepository(config);


main()
