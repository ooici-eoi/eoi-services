__author__ = 'timgiguere'

import tempfile
import os
from netCDF4 import Dataset
from nose.plugins.attrib import attr
from ion.eoi.agent.handler.dap_external_data_handler import DapExternalDataHandler
from pyon.util.unit_test import pop_last_call, PyonTestCase

@attr('UNIT', group='eoi')
class TestDapExternalDataHandler(PyonTestCase):

    def setUp(self):
        self.ds = create_test_data_set()

    def addCleanup(self):
        os.remove(self.tmp_name)

    def create_test_data_set(self):

        tmp_handle, self.tmp_name = tempfile.mkstemp(suffix='.nc')

        ds = Dataset(self.tmp_name, 'w')
        group = ds.createGroup('samplegroup')
        obs = group.createDimension('obs', None)
        lat = group.createDimension('lat', 73)
        lon = group.createDimension('lon', 144)

        time = group.createVariable('time','f8',('obs',))
        time.standard_name = "time"
        time.long_name = "time"
        time._FillValue = -1
        time.units = "seconds since 1970-01-01 00:00:00"
        time.comment = "estimated time of observation"

        latitudes = group.createVariable('latitude','f4',('lat',))
        latitudes.standard_name = "latitude"
        latitudes.long_name = "station latitude"
        latitudes.units = "degrees_north"

        longitudes = group.createVariable('longitude','f4',('lon',))
        longitudes.standard_name = "longitude"
        longitudes.long_name = "station longitude"
        longitudes.units = "degrees_east"

        pres = group.createVariable('pres','f4',('obs',))
        pres.standard_name = "sea_water_pressure"
        pres.long_name = "pressure level"
        pres.units = "dbar"
        pres.coordinates = "time lon lat z"
        pres.instrument_name = "SBE52MP"
        pres.instrument_serial_number = "12"
        pres.instrument_mount = "mounted_on_moored_profiler"
        pres.ancillary_variables = "pres_qc"

        ds.close()

        os.close(tmp_handle)

        return ds

    def test_get_signature(self):
        dsrc = IonObject("DataSource", name="test")
        dsrc.base_data_url = ""

        dataDesc = IonObject("DapDatasetDescription", name="test")
        dataDesc.dataset_path = self.tmp_name
        dataDesc.temporal_dimension = "time"

        dsh = DapExternalDataHandler(data_provider=None, data_source=dsrc, ext_dataset=None, dataset_desc=dataDesc, update_desc=None)

        dsh.ds = self.ds
        signature = dsh.get_signature()
        print signature

