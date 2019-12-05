#!/usr/bin/env python
# coding=utf-8

import os
import sys
import logging
from logging.handlers import RotatingFileHandler

if hasattr(sys, 'frozen'):
    _srcfile = "utils%slog%s" % (os.sep, __file__[-4:])
elif __file__[-4:].lower() in ['.pyc', '.pyo']:
    _srcfile = __file__[:-4] + '.py'
else:
    _srcfile = __file__
_srcfile = os.path.normcase(_srcfile)


def get_call_filename(strip_suffix=False):
    try:
        raise Exception
    except:
        current_frame = sys.exc_info()[2].tb_frame.f_back
    while hasattr(current_frame, "f_code"):
        co = current_frame.f_code
        filename = os.path.normcase(co.co_filename)
        if filename == _srcfile:
            current_frame = current_frame.f_back
            continue
        path, file = os.path.split(co.co_filename)
        filename = file or path
        if not strip_suffix:
            return filename
        else:
            return filename[0:filename.rfind(".")]
    return ""


def log_init(logconfig):
    logger = logging.getLogger()
    logger.setLevel(logconfig["level"])
    log_file = logconfig.get("file") or "%s.log" % get_call_filename(True)
    log_path = logconfig["path"].rstrip("/")
    log_file = os.path.join(log_path, log_file)
    if not os.path.exists(log_path):
        os.makedirs(log_path)
    rotate_handler = RotatingFileHandler(
        log_file,
        maxBytes=logconfig["size"],
        backupCount=logconfig["count"]
    )
    rotate_handler.setLevel(logconfig["level"])
    formatter = logging.Formatter(logconfig["format"])
    rotate_handler.setFormatter(formatter)
    logger.addHandler(rotate_handler)

    cosole_handler = logging.StreamHandler()
    cosole_handler.setLevel(logconfig["level"])
    cosole_handler.setFormatter(formatter)
    logger.addHandler(cosole_handler)

