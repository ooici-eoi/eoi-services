__author__ = 'timgiguere'

import tempfile
import os
from netCDF4 import Dataset
from nose.plugins.attrib import attr
from ion.eoi.agent.handler.dap_external_data_handler import DapExternalDataHandler
from pyon.util.unit_test import pop_last_call, PyonTestCase
from pyon.core.bootstrap import IonObject
import unittest

@attr('UNIT', group='eoi')
class TestDapExternalDataHandler(PyonTestCase):

    def setUp(self):
        pass

    #def addCleanup(self):
        #os.remove(self.tmp_name)

    def create_test_data_set_1(self, tmp_handle, tmp_name):

        ds = Dataset(tmp_name, 'w')
        group = ds.createGroup('samplegroup')
        obs = group.createDimension('obs', None)
        lat = group.createDimension('lat', 73)
        lon = group.createDimension('lon', 144)

        time = group.createVariable('time','f8',('obs',),fill_value=-1)
        time.standard_name = "time"
        time.long_name = "time"
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

    def get_temp_file(self):
        return tempfile.mkstemp(suffix='.nc')

    def create_test_data_set_2(self, tmp_handle, tmp_name):
        ds = Dataset(tmp_name, 'w')
        group = ds.createGroup('samplegroup')
        obs = group.createDimension('obs', None)
        lat = group.createDimension('lat', 73)
        lon = group.createDimension('lon', 144)

        time = group.createVariable('time','f8',('obs',),fill_value=-1)
        time.standard_name = "time"
        time.long_name = "time"
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

    def create_data_set_handler(self, tmp_name):
        dsrc = IonObject("DataSource", name="test")
        dsrc.base_data_url = ""

        dataDesc = IonObject("DapDatasetDescription", name="test")
        dataDesc.dataset_path = tmp_name
        dataDesc.temporal_dimension = "time"

        dsh = DapExternalDataHandler(data_provider=None, data_source=dsrc, ext_dataset=None, dataset_desc=dataDesc, update_desc=None)
        return dsh

    def test_get_signature(self):
        tmp_handle, tmp_name = self.get_temp_file()
        ds = self.create_test_data_set_1(tmp_handle, tmp_name)
        dsh = self.create_data_set_handler(tmp_name)
        dsh.ds = ds
        signature = dsh.get_signature()
        self.assertTrue(signature != "")

    def test_compare(self):
        tmp_handle_1, tmp_name_1 = self.get_temp_file()
        ds_1 = self.create_test_data_set_1(tmp_handle_1, tmp_name_1)
        dsh_1 = self.create_data_set_handler(tmp_name_1)
        dsh_1.ds = ds_1
        tmp_handle_2, tmp_name_2 = self.get_temp_file()
        ds_2 = self.create_test_data_set_2(tmp_handle_2, tmp_name_2)
        dsh_2 = self.create_data_set_handler(tmp_name_2)
        dsh_2.ds = ds_2
        dsh_1.compare(dsh_2)

if __name__ == '__main__':
    unittest.main()