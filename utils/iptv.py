# -*- coding: utf-8 -*-
import os
import json
import requests
import logging
import traceback
from utils.singleton import Singleton


iptv_instance = None


class IptvList:
    __metaclass__ = Singleton

    def __init__(self, url, meta_file):
        self._url = url
        self._ip_tv = {}
        self._meta_file = meta_file

    def save(self):
        s = json.dumps(self._ip_tv)
        with open(self._meta_file, 'w') as f:
            f.write(s)

    def load(self):
        if not (os.path.exists(self._meta_file) and os.path.isfile(self._meta_file)):
            self._load_from_net()
        else:
            self._load_from_file()

    def _load_from_file(self):
        try:
            with open(self._meta_file) as rfd:
                self._ip_tv = json.load(rfd)
                logging.info(f"load ip tv from file {self._meta_file}")
        except:
            logging.error(f"error found when load {self._meta_file}, error {traceback.format_exc()}")
            return

    def _load_from_net(self):
        try:
            resp = requests.get(self._url)
            if resp.status_code != 200:
                logging.error(f"load ip tv list http {resp.status_code}")
                return

            resp_json = resp.json()
            """
            {
            "id":8156,
            "chId":"57e0d37a-01e5-4316-8f48-a6db38b9f7c6-20190426184212",
            "name":"中国IPTV-山东V1",
            "image":"system_info/std_channel/77d08b75-97ba-679.jpeg",
            "createdAt":"2019-04-26 18:42:38",
            "updatedAt":"2019-04-28 16:58:02",
            "alias":[],"checked":true,"operator":"中国移动","sourceType":"IPTV","launcher":""}
            """
            for ipi_tv_info in resp_json['data']:
                channel_id = ipi_tv_info['chId']
                self._ip_tv[channel_id] = {
                    "chId": channel_id,
                    "name": ipi_tv_info['name'],
                    "operator": ipi_tv_info['operator'],
                    "sourceType": ipi_tv_info['sourceType']
                }
        except:
            logging.error(f"load exception: {traceback.format_exc()}")
            raise

    def query_id_info(self, std_id):
        q_id = std_id
        try:
            info = self._ip_tv[q_id]
            return [info['operator'], info['sourceType']]
        except:
            logging.debug(f"ERROR query std id failed {q_id}")
            return ['UNKNOWN', 'UNKNOWN']


def ip_tv_init(url, local):
    global iptv_instance
    iptv_instance = IptvList(url, local)
    iptv_instance.load()
    iptv_instance.save()


def query_broadband(std_id):
    return iptv_instance.query_id_info(std_id)
