import os
from uuid import uuid1


def get_mac():
    uid = str(uuid1())
    return uid.rsplit("-", 1)[1].upper()


def mkdir(s):
    try:
        os.makedirs(s, exist_ok=True)
    except:
        pass
