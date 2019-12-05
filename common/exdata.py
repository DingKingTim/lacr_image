from collections import namedtuple


AcrImgMeta = namedtuple("AcrImgMeta", ['date', 'meta', 'path', "topic", "part", "offset"])
SynPara = namedtuple("SynPara", ["brokers", "topic", "partitions", ])
