import datetime
import itertools
import json
import os
import re
import shutil
from typing import Optional

from aiohttp import RequestInfo

from rucio.db.sqla.constants import RequestState

try:
    from json.decoder import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError

import functools
import logging
import time
import traceback

from urllib.parse import urlparse  # py3
import uuid

import requests
from configparser import NoOptionError, NoSectionError
from json import loads
from requests.adapters import ReadTimeout
from requests.packages.urllib3 import disable_warnings  # pylint: disable=import-error

from dogpile.cache.api import NoValue

from rucio.common.cache import make_region_memcached
from rucio.common.config import config_get, config_get_bool
from rucio.common.constants import FTS_JOB_TYPE, FTS_STATE
from rucio.common.exception import (
    TransferToolTimeout,
    TransferToolWrongAnswer,
    DuplicateFileTransferSubmission,
)
from rucio.common.utils import APIEncoder, chunks, set_checksum_value
from rucio.core.rse import get_rse_supported_checksums_from_attributes
from rucio.core.oidc import get_token_for_account_operation
from rucio.core.monitor import record_counter, record_timer, MultiCounter
from rucio.transfertool.transfertool import Transfertool, TransferToolBuilder

logging.getLogger("requests").setLevel(logging.CRITICAL)
disable_warnings()


class SimpleTransfertool(Transfertool):
    """
    Simple implementation of a Rucio transfertool

    This is not actually used anywhere at the moment
    """

    external_name = 'simplett'

    def __init__(self, external_host, logger=logging.log):
        super().__init__(external_host, logger)

    @classmethod
    def submission_builder_for_path(cls, transfer_path, logger=logging.log):
        return TransferToolBuilder(cls, external_host="Simple Transfertool")

    def group_into_submit_jobs(self, transfers):
        return [
            {
                "transfers": list(itertools.chain.from_iterable(transfers)),
                "job_params": {},
            }
        ]

    def request_id_fn(self, requestid):
        return f"simplett-request-{requestid}.json"

    def submit(self, files, job_params, timeout=None):
        logging.info("%s sumbitting files %s with job_params %s", self, files, job_params)

        requestid = str(uuid.uuid1())

        for file in files:
            logging.info("file: %s", str(file))
            logging.info("--- %s", file.dest_url)
            for src in file.legacy_sources:
                logging.info("--- %s", src)

        with open(self.request_id_fn(requestid), "w") as f:
            json.dump([
                file.to_json() for file in files
            ], f)
            
        return requestid

    def cancel(self, transfer_ids, timeout=None):
        return True

    def update_priority(self, transfer_id, priority, timeout=None):
        return True

    def transfer_file(self, src: str, dest: str):
        logging.info("tranferring %s => %s", src, dest)

        if not (src.startswith("posix://") and dest.startswith("posix://")):
            raise NotImplementedError("for now, we only support posix pfns!")
    
        src_fn = re.sub('^posix://posix', '', src) # TODO: exclude host
        dest_fn = re.sub('^posix://posix', '', dest)
    
        os.makedirs(os.path.dirname(dest_fn), exist_ok=True)

        logging.info("src_fn %s", src_fn)        
        logging.info("dest_fn %s", dest_fn)        

        shutil.copyfile(src_fn, dest_fn)


    def transfer_now(self, transfer):        
        logging.info("transfer now for record: %s", json.dumps(transfer, indent=4, sort_keys=True))
        for transfer_file in transfer:
            self.transfer_file(transfer_file['legacy_sources'][0][1], transfer_file['dest_url'])



    def query(self, transfer_ids: list, details=False, timeout=None, transfers_by_eid=None):
        if transfers_by_eid is None:
            transfers_by_eid = {}

        for transfer_id in transfer_ids:
            with open(self.request_id_fn(transfer_id)) as f:
                transfer = json.load(f)                
                self.transfer_now(transfer)

        def map_some_fields(d):
            return {
                {
                    'source_rse_id': 'src_rse_id',
                    'source_rse': 'src_rse',
                    'source_url': 'src_url',
                    'dest_rse_id': 'dst_rse_id',
                    'dest_rse': 'dst_rse',
                    'dest_url': 'dst_url',                    
                }.get(k, k): v for k, v in d.items()
            }

        return {
            transfer_id: "SUCCEEDED" for transfer_id in transfer_ids
        }

    def bulk_query(self, transfer_ids: list, timeout: Optional[float]):
        return self.query(transfer_ids)
        
