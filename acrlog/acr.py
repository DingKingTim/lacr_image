import os
import base64
import asyncio
import logging
import threading
import traceback
from utils.system import mkdir
from utils.iptv import query_broadband
from aiohttp import FormData, ClientSession
from synqueue.status.redisstatus import RedisStatus


class AcrLog(threading.Thread):
    def __init__(self, **kwargs):
        super().__init__()
        self._chan = None
        self._pid = os.getpid()
        self._acr_para = kwargs["acr_para"]
        self.acr_queue = kwargs["acr_queue"]
        self.acr_url = self._acr_para.url
        self.log_dir = self._acr_para.log_dir
        self.worker_num = self._acr_para.workers
        self._non_ip_tv = ["UNKNOWN", "UNKNOWN"]
        self.stat_cfg = kwargs["stat_para"]
        self.status = None
        self.offset = {}
        self._lock = None
        self._running = True
        self.name = "acr"

    def close(self):
        logging.info(f"thread:{self.name} is ready to stop")
        self._running = False

    def run(self):
        try:
            asyncio.run(self._run())
        except asyncio.CancelledError:
            logging.info(f"thread:{self.name} stopped")
        except:
            logging.error(f"thread:{self.name} exception exit {traceback.format_exc()}")

    async def _run(self):
        self.status = RedisStatus(
            prefix=self.stat_cfg.prefix,
            host=self.stat_cfg.host,
            pwd=self.stat_cfg.pwd,
            db=self.stat_cfg.db)
        await self.status.open()
        self._lock = asyncio.Lock(loop=asyncio.get_running_loop())
        self._chan = asyncio.Queue(maxsize=5000, loop=asyncio.get_running_loop())
        tasks = [
            asyncio.ensure_future(self._worker(
                f"Cor-{i}",
                f"log-{self._pid}.{i}")) for i in range(self.worker_num)
        ]
        tasks.append(asyncio.ensure_future(self._detect()))
        tasks.append(asyncio.ensure_future(self._distribute()))
        await asyncio.gather(*tasks)

    async def _syn_offset(self):
        for k in self.offset:
            await self.status.update(topic=self.offset[k]["topic"],
                                     partition=self.offset[k]["part"],
                                     offset=self.offset[k]["offset"])

    async def _detect(self):
        seconds = 0

        while self._running:
            logging.info(f"thread:{self.name} running status {self._running}")
            await asyncio.sleep(5)
            seconds += 1
            if seconds < 600:
                continue

            seconds = 0
            await self._syn_offset()

        async with self._lock:
            await self._syn_offset()

        logging.info(f"thread:{self.name} detect will stop thread, saved status: {self.offset}")
        raise asyncio.CancelledError

    async def _distribute(self):
        try:
            while self._running:
                msg = await self.acr_queue.get()
                await self._chan.put(msg)
        except asyncio.CancelledError:
            logging.info(f"thread:{self.name} distribute stop")
            return
        except:
            logging.error(f"acr distribute exception {traceback.format_exc(1)}")

    async def _worker(self, name, log_name):
        logging.info(f"thread:{self.name} {name} start ...")

        try:
            lines = 0
            cur_fd = None
            cur_date = None

            async with ClientSession() as session:
                while self._running:
                    msg = await self._chan.get()
                    resp = await self._classify(session, msg.path)
                    if resp[2] == "UNKNOWN":
                        resp += self._non_ip_tv
                    else:
                        resp += query_broadband(resp[2])

                    if cur_fd is None or cur_date != msg.date:
                        if cur_fd:
                            cur_fd.close()

                        cur_date = msg.date
                        log_path = os.path.join(self.log_dir, cur_date)
                        mkdir(log_path)
                        log_file = os.path.join(log_path, log_name)
                        cur_fd = open(log_file, mode='a')
                        lines = 0

                    resp_meta = msg.meta[:-3] + resp + msg.meta[-3:]
                    line = ",".join(resp_meta)
                    cur_fd.write(f"{line}\n")
                    if lines % 10 == 0:
                        cur_fd.flush()

                    async with self._lock:
                        key = f"{msg.topic}_{msg.part}"
                        self.offset[key] = {
                            "topic": msg.topic,
                            "part": msg.part,
                            "offset": msg.offset
                        }
        except asyncio.CancelledError:
            logging.info(f"thread:{self.name} {name} stop work")
        except:
            logging.error(f"thread:{self.name} {name} error found cloud acr {traceback.format_exc()}")

        logging.info(f"thread:{self.name} {name} over ...")

    async def _classify(self, session, img_file):
        try:
            form_data = FormData()
            form_data.add_field("imagefile", open(img_file, 'rb'),
                                filename=os.path.basename(img_file))
            async with session.post(self.acr_url, data=form_data) as resp:
                if resp.status == 200:
                    resp_data = await resp.json()
                    name = str(base64.b64decode(resp_data['name']), encoding="utf-8")
                    return [name, resp_data['type'], resp_data['std_channel_id'], str(resp_data['caffe_scores'])]
                else:
                    return ["UNKNOWN", "UNKNOWN", "UNKNOWN", "0."]
        except asyncio.CancelledError:
            logging.info(f"thread:{self.name} classify stop")
            return ["UNKNOWN", "UNKNOWN", "UNKNOWN", "0."]
        except:
            logging.error(f"thread:{self.name} error found cloud acr {traceback.format_exc()}")
            return ["UNKNOWN", "UNKNOWN", "UNKNOWN", "0."]
