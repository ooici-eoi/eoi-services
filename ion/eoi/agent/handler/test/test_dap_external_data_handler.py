__author__ = 'timgiguere'

import tempfile
import os
from netCDF4 import Dataset
from nose.plugins.attrib import attr
from ion.eoi.agent.handler.dap_external_data_handler import DapExternalDataHandler, DatasetComparisonResult
from pyon.util.unit_test import pop_last_call, PyonTestCase
from pyon.core.bootstrap import IonObject
import unittest

@attr('UNIT', group='eoi')
class TestDapExternalDataHandler(PyonTestCase):

    _ds_list={}
    _ds1_sig=None

    def setUp(self):
        # TODO: use these to key the creation of various test datasets
        lst = ["DS1", "DS1_DUP", "DS_DIM_SIZE_CHANGED", "DS_VAR_ATT_CHANGED", "DS_GLOBAL_ATT_CHANGED"]

        for key in lst:
            th,tn=self.get_temp_file()
            ds = self.create_tst_data_set_1(th,tn, key)
            self._ds_list[key]=tn, th, ds

        self._ds1_sig = ('601c1b036c9d2590bd916d5b25716bcdd5ad3209',
                        {'dims': ('66b17e07d49eb154e5e1bcad6ed6dddf5ac55829',
                            {'lat': 'ea22bf57c7281557e134ded98195a229668b9381',
                            'lon': '4d6ae4cf654eea344c7f681cb3f17f63f0bdfaad',
                            'obs': 'b66e1e67cd63f645eba5ce523e87a3ffdb7597fb'}),
                        'gbl_atts': ('09a13128e45c6e1909c4b762bdd0d84b9736cfbe',
                            {u'history': '70d1ff82dbc7cc7a0972f67ab75ce93a184f160f',
                             u'creator': '3c5b4941b7b4594facec9eb943e4d3c5401d882a'}),
                        'vars': ('150fc2d60d2b7d913888a0abff40af99933f11e3',
                            {'latitude': ('0198dc9142964d3a9e3b68608cd83ee45d915193',
                                {u'units': '84f9eb7a658bfac37e07152c3ea75548fee6f512',
                                u'long_name': '648b097ed4fff175013ce05cbb03ddb97b957774',
                                u'standard_name': '5fcccdcf1d079c4a85c92c6fe7c8d29a27e49bed'}),
                            'pres': ('eab02b31074124866d163a09d2e93f4bd8baa418',
                                {u'ancillary_variables': '91ddcd9068fc8760336c20c1519d22759fe5e4a9',
                                u'coordinates': 'e213e825d010c1bb56f66488d0c17cd0f65564d3',
                                u'long_name': '2b58cfcccad9f8fa8188906ddfdc870d39e9dabc',
                                u'standard_name': '6581778bf3246aa22030ebd47b62edf1cf8d40d2',
                                u'instrument_name': 'c4658becfa0addf7c4b86c870a2784a33c71fe46',
                                u'units': 'de524ecedd736cf05f6f9f39e7ef1b9e60d5855e',
                                u'instrument_mount': '355d21804150a22bf011508a4ff8da69e17bf60e',
                                u'instrument_serial_number': '7b52009b64fd0a2a49e6d8a939753077792b0554'}),
                            'longitude': ('73a46062642560b595945b52d8429a5003f204ee',
                                {u'units': 'ff371c19d6872ff49d09490ecbdffbe3e7af2be5',
                                u'long_name': 'fb1da283b45742604e37887bb83bfedb65e19bbc',
                                u'standard_name': 'd2a773ae817d7d07c19d9e37be4e792cec37aff0'}),
                            'time': ('3244a9baf3e4be8d9746b660fc140287ee6122f1',
                                {u'units': 'd779896565174fdacafb4c96fc70455a2ac7d826',
                                u'_FillValue': '3b75a28be8e68812da8cc1a9c77f1f04dfa30815',
                                u'standard_name': '714eea0f4c980736bde0065fe73f573487f08e3a',
                                u'comment': 'd4bd716c721dd666ab529575d2ccaea4f0867dd9',
                                u'long_name': '714eea0f4c980736bde0065fe73f573487f08e3a'})})})


    def tearDown(self):
        for key in self._ds_list:
            os.remove(self._ds_list[key][0])

    def create_tst_data_set_1(self, tmp_handle, tmp_name, key):
        ds = Dataset(tmp_name, 'w')
        obs = ds.createDimension('obs', None)
        lat = ds.createDimension('lat', 73)
        lon_len = 144
        if key is "DS_DIM_SIZE_CHANGED":
            lon_len = 150
        lon = ds.createDimension('lon', lon_len)

        ds.creator = "ocean observing initiative"
        ds.history = "first history entry"
        if key is "DS_GLOBAL_ATT_CHANGED":
            ds.history = ds.history + "; second history entry"

        time = ds.createVariable('time','f8',('obs',),fill_value=-1)
        time.standard_name = "time"
        time.long_name = "time"
        time.units = "seconds since 1970-01-01 00:00:00"
        time.comment = "estimated time of observation"

        latitudes = ds.createVariable('latitude','f4',('lat',))
        latitudes.standard_name = "latitude"
        if key is "DS_VAR_ATT_CHANGED":
            latitudes.long_name = "station has changed"
        else:
            latitudes.long_name = "station latitude"
        latitudes.units = "degrees_north"

        longitudes = ds.createVariable('longitude','f4',('lon',))
        longitudes.standard_name = "longitude"
        longitudes.long_name = "station longitude"
        longitudes.units = "degrees_east"

        pres = ds.createVariable('pres','f4',('obs',))
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

    def create_tst_data_set_2(self, tmp_handle, tmp_name):
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

    def create_tst_data_set_handler(self, tmp_name):
        dsrc = IonObject("DataSource", name="test")
        dsrc.base_data_url = ""

        dataDesc = IonObject("DapDatasetDescription", name="test")
        dataDesc.dataset_path = tmp_name
        dataDesc.temporal_dimension = "time"

        dsh = DapExternalDataHandler(data_provider=None, data_source=dsrc, ext_dataset=None, dataset_desc=dataDesc, update_desc=None)
        return dsh

    def test_get_signature(self):
        tn,th,ds=self._ds_list["DS1"]
        dsh = self.create_tst_data_set_handler(tn)
        dsh.ds = ds
        signature = dsh.get_signature()
#        raise StandardError(signature)
#        self.assertTrue(signature != "")
        self.assertEqual(signature, self._ds1_sig)

    def test_compare_identical(self):
        tn_1,th_1,ds_1 = self._ds_list["DS1"]
        dsh_1 = self.create_tst_data_set_handler(tn_1)
        dsh_1.ds = ds_1

        tn_2,th_2,ds_2 = self._ds_list["DS1_DUP"]
        dsh_2 = self.create_tst_data_set_handler(tn_2)
        dsh_2.ds = ds_2
        
        dcr = dsh_1.compare(dsh_2)
        self.assertTrue(dcr.get_result()[0], "EQUAL")
    
    def test_compare_global_attribute_changed(self):
        tn_1,th_1,ds_1 = self._ds_list["DS1"]
        dsh_1 = self.create_tst_data_set_handler(tn_1)
        dsh_1.ds = ds_1

        tn_2,th_2,ds_2 = self._ds_list["DS_GLOBAL_ATT_CHANGED"]
        dsh_2 = self.create_tst_data_set_handler(tn_2)
        dsh_2.ds = ds_2
        
        dcr = dsh_1.compare(dsh_2)
        self.assertTrue(dcr.get_result()[0], "MOD_GBL_ATTS")

    def test_compare_dim_size_changed(self):
        tn_1,th_1,ds_1 = self._ds_list["DS1"]
        dsh_1 = self.create_tst_data_set_handler(tn_1)
        dsh_1.ds = ds_1

        tn_2,th_2,ds_2 = self._ds_list["DS_DIM_SIZE_CHANGED"]
        dsh_2 = self.create_tst_data_set_handler(tn_2)
        dsh_2.ds = ds_2

        dcr = dsh_1.compare(dsh_2)
        self.assertTrue(dcr.get_result()[0], "MOD_DIMS")
        

if __name__ == '__main__':
    unittest.main()