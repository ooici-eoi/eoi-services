__author__ = 'cmueller'

from pyon.public import IonObject
from examples.eoi.test_base import *
from ion.eoi.agent.handler.dap_external_data_handler import DapExternalDataHandler
from datetime import datetime, timedelta

if __name__ == '__main__':

    ret = get_dataset(GHPM)

    dsh = DapExternalDataHandler(ret[EXTERNAL_DATA_PROVIDER], ret[DATA_SOURCE], ret[EXTERNAL_DATA_SET], ret[DAP_DS_DESC])

    td=timedelta(days=-1)
    edt=datetime.utcnow()
    sdt=edt+td

    
    req={}
    req["start_time"] = sdt
    req["end_time"] = edt

    req_obj = IonObject("ExternalDataRequest", req)

    resp = dsh.acquire_new_data(req_obj)
#    print resp