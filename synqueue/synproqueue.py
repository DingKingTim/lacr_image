import os
import json
import base64
import logging
import asyncio
import datetime
import aiofiles
import traceback
from utils.system import mkdir
from common.exdata import AcrImgMeta
from common.constant import MsgField
from synqueue.status.redisstatus import RedisStatus
from aiokafka import AIOKafkaConsumer, TopicPartition


def _value_serializer(value):
    try:
        return json.loads(value)
    except:
        logging.error(f"value serializer parse value failed {traceback.format_exc()}")
        return None


class SynProQueue:

    def __init__(self, **kwargs):
        self._running = True
        self.mac = kwargs["mac"]
        self.name = f"SQ-{os.getpid()}"
        self.acr_queue = kwargs["acr_queue"]

        self.partitions = {}
        self.queue_para = kwargs["syn_para"]
        self.topic = self.queue_para.topic
        self.queue_brokers = self.queue_para.brokers

        self.img_para = kwargs["img_para"]
        self.archive_dir = self.img_para.archive_dir
        self.stat_cfg = kwargs["stat_para"]
        self.status = None
        self.name = "syn_queue"

    def close(self):
        logging.info(f"thread:{self.name} is ready to stop")
        self._running = False

    def run(self):
        try:
            asyncio.run(self._run())
        except asyncio.CancelledError:
            logging.info(f"thread:{self.name} safe stop")
        except:
            logging.error(f"thread:{self.name} exception stop {traceback.format_exc()}")
            return

    async def _run(self):
        self.status = RedisStatus(
            prefix=self.stat_cfg.prefix,
            host=self.stat_cfg.host,
            pwd=self.stat_cfg.pwd,
            db=self.stat_cfg.db)
        await self.status.open()

        for part in self.queue_para.partitions:
            topic_partition = TopicPartition(self.topic, part)
            offset = await self.status.read(self.topic, partition=part)
            logging.info(f"thread:{self.name} {self.topic} read offset {offset}")
            self.partitions[topic_partition] = offset

        await self.status.close()

        # tasks = [asyncio.ensure_future(self._queue_client(partition)) for partition in self.partitions]
        tasks = [asyncio.ensure_future(self._pull(self.partitions)),
                 asyncio.ensure_future(self._detect())]
        await asyncio.gather(*tasks)

    async def _detect(self):
        while self._running:
            logging.info(f"thread:{self.name} detect status {self._running}")
            await asyncio.sleep(5)

        logging.info(f"thread:{self.name} detect will stop thread")
        raise asyncio.CancelledError

    async def _pull(self, partitions):
        logging.info(f"thread:{self.name} pull start... ")

        try:
            consumer = AIOKafkaConsumer(
                loop=asyncio.get_running_loop(),
                bootstrap_servers=self.queue_brokers,
                value_deserializer=_value_serializer,
                enable_auto_commit=True)

            await consumer.start()
            consumer.assign(list(partitions.keys()))

            for k in partitions:
                offset = partitions[k]
                if offset > 0:
                    consumer.seek(k, offset)

            async for msg in consumer:
                if self._running is False:
                    logging.info(f"thread:{self.name} stop running")
                    break

                acr_meta = await self._process(msg)
                if acr_meta is not None:
                    logging.debug(f"thread:{self.name} pull msg {msg.topic}, {msg.partition}")
                    await self.acr_queue.put(acr_meta)

        except asyncio.CancelledError:
            logging.info(f"thread:{self.name} pull stop")
        except:
            logging.error(f"thread:{self.name} exception stop {traceback.format_exc()}")

        logging.info(f"thread:{self.name} over... ")

    async def _process(self, message):
        msg = message.value
        try:
            gz_id = msg["gzid"]
            img_name = msg["img_name"]
            device_id = msg["device_id"]
            device_time = msg["create_time"]
            logging.info(f"thread:{self.name}, {message.partition},"
                         f" {message.offset}, {device_time}, {gz_id}, {device_id}, {img_name}")
            img_code = base64.b64decode(msg["img_code"])

            create_time = datetime.datetime.strptime(msg["create_time"], "%Y-%m-%d %H:%M:%S")
            save_time = create_time - datetime.timedelta(hours=2)
            save_date = save_time.strftime("%Y-%m-%d")
            dev_dir = os.path.join(self.archive_dir, self.mac, save_date, f"{gz_id}_900000_{device_id}_{save_date}")
            if not (os.path.exists(dev_dir) and os.path.isdir(dev_dir)):
                mkdir(dev_dir)

            list_csv = os.path.join(dev_dir, self.img_para.dev_list)
            img_file = os.path.join(dev_dir, f"{img_name}")

            meta = []
            for key in MsgField:
                meta.append(f"{msg.get(key, '')}")
            meta[0] = meta[0].rsplit(".", 1)[0]
            meta_line = "\t".join(meta)

            async with aiofiles.open(img_file, 'wb') as wf:
                await wf.write(img_code)

            async with aiofiles.open(list_csv, mode='a') as wf:
                await wf.write(f"{meta_line}\n")

            return AcrImgMeta(date=save_date, meta=meta, path=img_file,
                              topic=message.topic, part=message.partition, offset=message.offset)
        except asyncio.CancelledError:
            logging.info(f"thread:{self.name} process safe stop")
        except:
            logging.error(f"thread:{self.name} process msg failed exception {traceback.format_exc()}")
            return None
