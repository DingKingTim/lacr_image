import os
import signal
import logging
import traceback
import multiprocessing
from utils import aioqueue
from acrlog.acr import AcrLog
from utils.log import log_init
from utils.iptv import ip_tv_init
from common.exdata import SynPara
from utils.system import get_mac, mkdir
from synqueue.synthread import SynThread
from config import Config


def _process(kwargs):
    pid = os.getpid()
    mac = kwargs["mac"]
    log = kwargs["log"]
    syn_para = kwargs["syn_para"]
    acr_para = kwargs["acr_para"]
    img_para = kwargs["img_para"]
    ip_tv_para = kwargs["ip_tv"]
    stat_para = kwargs["stat_cfg"]

    acr_queue = aioqueue.Queue(max_size=acr_para.queue_cap)

    mkdir(log["path"])
    log_init(log)

    ip_tv_init(ip_tv_para.url, ip_tv_para.local)

    threads = [
        SynThread(
            mac=mac,
            syn_para=syn_para,
            acr_para=acr_para,
            img_para=img_para,
            acr_queue=acr_queue,
            stat_para=stat_para,
        ),

        AcrLog(
            acr_para=acr_para,
            acr_queue=acr_queue,
            stat_para=stat_para,
        )
    ]

    def _signal(sig, frame):
        logging.info(f"{os.getpid()} recv signal {sig} {frame}")
        for w in threads:
            w.close()

    signal.signal(signal.SIGINT, _signal)
    signal.signal(signal.SIGTERM, _signal)
    signal.signal(signal.SIGABRT, _signal)

    logging.info(f"process {pid} start... ")

    try:
        for task in threads:
            task.start()

        for task in threads:
            task.join()
    except:
        logging.error(f"process {pid} over... ")

    logging.info(f"process {pid} end... ")


def run(cfg_file):
    mac = get_mac()
    main_pid = os.getpid()
    project_dir = os.path.split(os.path.realpath(__file__))[0]

    cfg = Config()
    cfg.parse(cfg_file)

    partition_dict = {}
    for i in range(cfg.syn_cfg.start, cfg.syn_cfg.end+1):
        key = i % cfg.env_cfg.process_num
        if key in partition_dict:
            partition_dict[key].append(i)
        else:
            partition_dict[key] = [i]

    processes = []

    def _signal(sig, frame):
        logging.info(f"{os.getpid()} recv signal")
        for work in processes:
            work.terminate()

    signal.signal(signal.SIGINT, _signal)
    signal.signal(signal.SIGTERM, _signal)
    signal.signal(signal.SIGABRT, _signal)

    try:
        for idx, partition in enumerate(range(cfg.env_cfg.process_num), 0):
            syn_para = SynPara(brokers=cfg.syn_cfg.brokers,
                               topic=cfg.syn_cfg.topic,
                               partitions=partition_dict[idx])

            log_cfg = cfg.log_cfg()
            log_cfg["path"] = os.path.join(project_dir, "log", f"log-{idx}")

            p = multiprocessing.Process(target=_process, group=None, args=({
                "mac": mac,
                "log": log_cfg,
                "syn_para": syn_para,
                "acr_para": cfg.acr_cfg,
                "img_para": cfg.img_cfg,
                "project_dir": project_dir,
                "ip_tv": cfg.ip_tv_cfg,
                "stat_cfg": cfg.stat_cfg
            }, ))
            p.start()
            processes.append(p)

        for p in processes:
            p.join()

    except:
        logging.error(f"mp-{main_pid} exception exit {traceback.format_exc()}")

    logging.info(f"mp-{main_pid} over... ")
