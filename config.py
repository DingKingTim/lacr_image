import json
from collections import namedtuple


IpTvCfg = namedtuple("IpTvCfg", ["url", "local"])
EnvCfg = namedtuple("EnvCfg", ["process_num", ])
SynCfg = namedtuple("SynCfg", ["brokers", "topic", "start", "end"])
AcrCfg = namedtuple("AcrCfg", ["queue_cap", "url", "log_dir", "workers"])
ImgCfg = namedtuple("ImgCfg", ["archive_dir", "dev_list", "meta_file"])
StatCfg = namedtuple("StatCfg", ["host", "pwd", "db", "prefix"])


class Config:

    def __init__(self):
        self.log = None
        self.env = None
        self.syn = None
        self.acr = None
        self.img = None
        self.stat = None
        self.ip_tv = None

    @property
    def env_cfg(self):
        return self.env

    @property
    def syn_cfg(self):
        return self.syn

    @property
    def acr_cfg(self):
        return self.acr

    @property
    def img_cfg(self):
        return self.img

    @property
    def stat_cfg(self):
        return self.stat

    @property
    def ip_tv_cfg(self):
        return self.ip_tv

    def log_cfg(self):
        return self.log

    def parse(self, file):
        with open(file) as rfd:
            cfg = json.load(rfd)

            self.env = EnvCfg(process_num=cfg["env"]["process_num"])

            self.syn = SynCfg(brokers=cfg["syn_queue"]["brokers"],
                              topic=cfg["syn_queue"]["topic"],
                              start=cfg["syn_queue"]["partition_start"],
                              end=cfg["syn_queue"]["partition_end"])

            self.acr = AcrCfg(queue_cap=cfg["acr"]["queue_cap"],
                              url=cfg["acr"]["url"],
                              log_dir=cfg["acr"]["log_dir"],
                              workers=cfg["acr"]["workers"])

            self.img = ImgCfg(archive_dir=cfg["img"]["archive_dir"],
                              dev_list=cfg["img"]["dev_list"],
                              meta_file=cfg["img"]["meta_file"])

            self.ip_tv = IpTvCfg(url=cfg["ip_tv"]["url"],
                                 local=cfg["ip_tv"]["local"])

            self.stat = StatCfg(host=cfg["status"]["host"],
                                pwd=cfg["status"]["pwd"],
                                db=cfg["status"]["db"],
                                prefix=cfg["status"]["prefix"])

            self.log = cfg["log"]
