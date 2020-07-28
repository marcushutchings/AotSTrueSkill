#!/usr/bin/python3
# Calculate all player's TrueSkill in Ashes of the Singularity: Escalation
# Can only reliably rate players using Ranked and Unranked queue matches
# Other matches don't declare whether AI is used or whether players had a resource bonus

# JSON - load config and read data
# HTTP Headers - for handling next request
# Multi-threading - to quickly process the records
# TrueSkill - to calculate player's skill
# MongoDB- store player documents

from collections import OrderedDict
import datetime
import dateutil
import dateutil.parser
import json
import pymongo
import requests

APP_CONFIG_FILE = "trueskill.conf"
GAME_TYPE_QUEUED = "1v1Ranked"

COL_RAW_MATCHES = "raw_matches"
COL_RANKED_MATCHES = "ranked_matches"
COL_PLAYERS = "players"


# TODO:
# Load all records back to date
# record last date
# API works backwards 0 = most recent game


# Use $addfields


# Build list of required matches
# Check for new matches
#  cur count - old count
# Check of incomplete matches in old set
#  only add if one day of created time

# Get last changed date
# Calculate one day back
# add filter to unfinished matches
# sort on match id

class TrueSkillConfig:
    config_file_path = ""
    baseurl = "";
    next_record_index = 0;
    db_name = ""
    expected_date_format = "2020-07-19T16:31:15.4750333Z"

    def __init__(self, config_file_path):
        with open(config_file_path) as fh:
            data = json.load(fh)

        self.config_file_path = config_file_path
        self.base_url = data['baseUrl']
        self.next_record_index = data['nextRecordIndex']
        self.db_name = data['localDbName']

        print("Base API URL is", self.base_url)
        print("Next record to get is", self.next_record_index)

    def save(self):
        with open(self.config_file_path, 'w') as fh:
            data = {}
            data['baseUrl'] = self.base_url
            data['nextRecordIndex'] = self.next_record_index
            data['localDbName'] = self.db_name
            json.dump(data, fh)


class Match:
    match_id = ""
    match_type = ""
    created_date = None
    match_state = 0
    json_data = {}
    is_ranked = False
    match_index = 0

    def __init__(self, json_data, match_index):
        self.json_data = json_data
        self.match_id = json_data["matchId"]
        self.match_type = json_data["dataString"]["type"]
        if isinstance(json_data["createDate"], str):
            self.created_date = dateutil.parser.isoparse(\
                dateutil.parser.parse(json_data["createDate"]).isoformat() )
        else:
            self.created_date = json_data["createDate"]
        self.match_state = int(json_data["matchStateId"])
        self.match_index = match_index
        self.is_ranked = self.match_type == GAME_TYPE_QUEUED \
                            and json_data["dataInteger"]["duration"] > 120

        print("Match", self.match_index, "(", self.match_id, ")")

    def RankedMatch(self):
        return self.is_ranked


class MatchPage:
    api_page_index = 0
    match_start_index = 0
    matches = []

    def __init__(self, page_index, data, match_start_index):
        self.matches = []
        self.page_index = page_index
        self.match_start_index = match_start_index

        match_page_data = json.loads(data)
        self.match_count = len(match_page_data)
        self._load_matches(match_page_data)

    def _load_matches(self, json_data):
        for match_index in range(0, len(json_data)):
            #for cur_data in json_data:
            cur_data = json_data[match_index]
            self.matches.append(Match(cur_data, self._get_match_index(match_index)))

    def _get_match_index(self, api_match_index):
        return self.match_start_index + (self.match_count - api_match_index) + (-1)

    def matches_count(self):
        return len(self.matches)


class MatchDataRemoteRepository:
    base_url = ""
    matches_already_loaded = 0
    total_matches = 0
    total_pages = 0
    match_pages = []
    json_data = []

    def __init__(self, config):
        self.base_url = config.base_url
        self.matches_already_loaded = config.next_record_index
        self._init_connection()

    def _init_connection(self):
        with requests.get(self.base_url) as response:
            if response.ok:
                self.total_matches = int(response.headers['X-Total'])
                self.total_pages = (self.total_matches // 50)
                if self.total_matches % 50: self.total_pages += 1
                print("Current number of matches played is", self.total_matches)

    def _get_match_page_index(self, api_page_index):
        first_match_index = self.total_matches - (api_page_index+1)*50
        match_page_index = (first_match_index // 50)
        if match_page_index < 0: match_page_index = 0
        return match_page_index

    def load_recent_matches(self, num_matches_to_load):
        if num_matches_to_load <= 0: return
        pages_to_load = self._calc_pages_to_load(num_matches_to_load)
        self._load_matches(pages_to_load)
        
    def _calc_pages_to_load(self, matches_to_load):
        pages_to_load = (matches_to_load // 50)
        if matches_to_load % 50: pages_to_load += 1
        return pages_to_load

    def _load_matches(self, pages_to_load):
        for page in range(0, pages_to_load):
            new_page = self._load_matches_page(page)
            #print("loaded", new_page.matches_count(), "matches")
            self.match_pages.append(new_page)
            #print("loaded", self.match_pages[page].matches_count(), "matches")

    def _load_matches_page(self, page):
        offset = page*50
        page_first_match_index = self.total_matches - (offset + 50)
        if page_first_match_index < 0: page_first_match_index = 0
        request_url = self.base_url + "?offset=" + str(offset)
        print("request is", request_url)
        with requests.get(request_url) as response:
            if response.ok:
                json_data = json.loads(response.content)
                self.json_data += json_data
                return MatchPage(page, response.content, page_first_match_index)
        return None

    def get_all_matches(self):
        matches = []
        for mp in self.match_pages:
            matches += mp.matches
        return matches


class MatchDataLocalRepository:
    client = None
    db = None
    col_raw_matches = None
    col_matches = None
    col_players = None
    last_updated = datetime.datetime.now()

    def __init__(self, db_name):
        self.client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.db = self.client[db_name]
        self.col_raw_matches = self.db[COL_RAW_MATCHES]
        self._update_db_update_timestamp()

    def put_raw_match(self, data):
        data['_id'] = data['matchId']
        self.col_raw_matches.insert_one(data)

    def put_raw_matches(self, matchlist):
        datalist = []
        for match in matchlist:
            query = {"_id": match.match_id}
            record_is_present = (self.col_raw_matches.find_one(query) is not None)
            if record_is_present: continue

            data = dict(match.json_data)
            data["_id"] = data["matchId"]
            data["matchIndex"] = match.match_index
            data["createDate"] = match.created_date
            data["dataString"]["type"] = match.match_type
            datalist.append(data)

        if len(datalist) > 0:
            self.col_raw_matches.insert_many(datalist)

    def get_recent_incomplete_matches(self):
        matches = []
        records = self._get_recent_incomplete_matches()
        for record in records:
            matches.append(Match(record, record['matchIndex']))
        return matches

    def _update_db_update_timestamp(self):
        match_record = self.col_raw_matches.find_one()
        if match_record:
            self.last_updated = Match(match_record, 0).created_date
        #if match_records.count_documents() > 0:
        #    most_recent_match = Match(match_records[0], 0)
        #    self.last_updated = most_recent_match.created_date
        print("Local DB last match time", self.last_updated)

    def _get_recent_incomplete_matches(self):
        seek_back_to = self.last_updated - datetime.timedelta(days=1)
        print("Getting incomplete matches between", self.last_updated, "and", seek_back_to)
        results = list(self.col_raw_matches.aggregate( \
                [ {"$match": {"matchStateId": {"$ne": 6}, "createDate": {"$gt": seek_back_to}}} \
                , {"$sort": OrderedDict([("matchIndex", 1)])} \
                ]))
        print("Results", results)
        return results

    def num_raw_matches(self):
        return self.col_raw_matches.count_documents({})


class RankableMatches:
    rankable_matches = []

    def __init__(self, match_repo):
        self._load_match_repo(match_repo)

    def _load_match_repo(self, match_repo):
        for p in match_repo.match_pages:
            self._load_match_page(p)

    def _load_match_page(self, match_page):
        for m in match_page.matches:
            if m.RankedMatch():
                self.rankable_matches.append(m)
                print("Rankable match found")

class UpdateLocalDatabase:
    localdb = None
    remoteapi = None

    def __init__(self, localdb, remoteapi):
        self.localdb = localdb
        self.remoteapi = remoteapi

    def update_local_db(self):
        matches_to_load = 0
        new_matches_to_load = self._get_new_match_count();
        print("New matches to load:", new_matches_to_load)

        # temp change to limit downloads during testing
        if new_matches_to_load > 100: new_matches_to_load = 100

        last_match_to_update = self._most_recent_match_to_update()
        if last_match_to_update:
            matches_to_load = self.localdb. last_match_to_update.match_index
            print("Update matches to index", last_match_to_update.match_index)
        else:
            matches_to_load = new_matches_to_load

        self.remoteapi.load_recent_matches(matches_to_load)
        self.localdb.put_raw_matches(self.remoteapi.get_all_matches())

    def _get_new_match_count(self):
        prev_match_count = self.localdb.num_raw_matches()
        new_match_count = self.remoteapi.total_matches

        matches_to_fetch = new_match_count - prev_match_count
        if matches_to_fetch < 0: matches_to_fetch = 0

        return matches_to_fetch

    def _most_recent_match_to_update(self):
        matches = self.localdb.get_recent_incomplete_matches()
        print("Matches to update:", len(matches))
        if len(matches) > 0: return matches[0]
        return None


def main():
    config = TrueSkillConfig(APP_CONFIG_FILE)
    local_repo = MatchDataLocalRepository(config.db_name)
    remote_api = MatchDataRemoteRepository(config);
    updater = UpdateLocalDatabase(local_repo, remote_api)

    updater.update_local_db()

    #rank_matches = RankableMatches(matches)
    config.next_record_index = local_repo.num_raw_matches()
    config.save()

main()
