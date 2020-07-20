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
GAME_TYPE_RANKED = "1v1Ranked"
GAME_TYPE_UNRANKED = "Unranked"

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

class Match:
    type = ""

    def __init__(self, json_data):
        self.type = json_data["dataString"]["type"]
        print("Added match type", self.type)


class MatchPage:
    page_index = 0
    matches = []

    def __init__(self, page_index, data):
        self.page_index = page_index
        self._load_matches(json.loads(data))

    def _load_matches(self, json_data):
        for cur_data in json_data:
            self.matches.append(Match(cur_data));

    def matches_count(self):
        return len(self.matches)


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
        print("loaded", self.match_pages[0].matches_count(), "matches")

    def _load_matches_page(self, page):
        request_url = self.base_url + "?offset=" + str(page*50)
        print("request is", request_url)
        with requests.get(request_url) as response:
            if response.ok:
                return MatchPage(page, response.content)
        return None


class MatchFilter:
    
    def is_valid(self, match_to_assess):
        if match_to_assess is None:
            return false

        match_is_ranked = (match_to_assess.type == GAME_TYPE_RANKED)
        match_is_unranked = (match_to_assess.type == GAME_TYPE_UNRANKED)

        return (match_is_ranked or match_is_unranked)


class RankableMatches:
    filter = MatchFilter()
    rankable_matches = []

    def __init__(self, match_repo):
        self._load_match_repo(match_repo)

    def _load_match_repo(self, match_repo):
        for p in match_repo.match_pages:
            self._load_match_page(p)

    def _load_match_page(self, match_page):
        for m in match_page.matches:
            is_rankable = self.filter.is_valid(m)
            print("Match rankable is", is_rankable)
            if is_rankable: self.rankable_matches.append(m)

def main():
    config = TrueSkillConfig(APP_CONFIG_FILE)
    matches = MatchDataRepository(config);
    rank_matches = RankableMatches(matches)


main()
