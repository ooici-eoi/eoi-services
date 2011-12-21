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
            th,tn=self.get_temp_file(prefix=(key + "_"))
            ds = self.create_tst_data_set(th,tn,key)
            self._ds_list[key]=tn, th, ds

        self._ds1_sig = ('644919a73b7738c8d7be01f5176e2b9276c3c2ba', {'dims': (
        '9e86329cc35148d33f94da5d9a45c4996b8ff756',
            {'lat': '17b0bff9fd5da05a17e27478f72d39216546eafa', 'lon': 'f115170f91587fb723e33ac1564eb021615acfaa',
             'time': '165058f1f201a8e9e455687940ce890121dd7853'}), 'gbl_atts': (
        '09a13128e45c6e1909c4b762bdd0d84b9736cfbe', {u'history': '70d1ff82dbc7cc7a0972f67ab75ce93a184f160f',
                                                     u'creator': '3c5b4941b7b4594facec9eb943e4d3c5401d882a'}), 'vars': (
        '12dd72b317a36bd6a66748bb9e9edbe8e5048b4c', {'latitude': ('d3681c3dd7d2903894e2a9bbc9e557bd000da504', {
            u'units': '84f9eb7a658bfac37e07152c3ea75548fee6f512',
            u'long_name': '648b097ed4fff175013ce05cbb03ddb97b957774',
            u'standard_name': '5fcccdcf1d079c4a85c92c6fe7c8d29a27e49bed'}), 'temp': (
        '38416f2e10419e0cf74964a1fedd511a1031b877', {u'units': '53f8a000a1f0bc4b59a178b0bcea46c9ef8ff160',
                                                     u'long_name': 'b7d4eadd3b0618f8164c53f1c31df4acad5db392',
                                                     u'standard_name': '8e8c717ded6a5973b95661d2f298e175c04b1624'}),
                                                     'longitude': ('5f1b3621ec1fc8af18e798488bf3a514bca638fb', {
                                                         u'units': 'ff371c19d6872ff49d09490ecbdffbe3e7af2be5',
                                                         u'long_name': 'fb1da283b45742604e37887bb83bfedb65e19bbc',
                                                         u'standard_name': 'd2a773ae817d7d07c19d9e37be4e792cec37aff0'}),
                                                     'time': ('036112bc95c35ba8ccff3ae54d77e76c881ad1af', {
                                                         u'comment': 'd4bd716c721dd666ab529575d2ccaea4f0867dd9',
                                                         u'_FillValue': '7984b0a0e139cabadb5afc7756d473fb34d23819',
                                                         u'long_name': '714eea0f4c980736bde0065fe73f573487f08e3a',
                                                         u'standard_name': '714eea0f4c980736bde0065fe73f573487f08e3a',
                                                         u'units': 'd779896565174fdacafb4c96fc70455a2ac7d826',
                                                         u'calendar': '4a78d3cb314a4e97cfe37eda5781f60b87f6145e'})})})


    def tearDown(self):
        for key in self._ds_list:
            os.remove(self._ds_list[key][0])
#        pass

    def create_tst_data_set(self, tmp_handle, tmp_name, key):
        import numpy
        ds = Dataset(tmp_name, 'w')
        time = ds.createDimension('time', None)
        lat = ds.createDimension('lat', 80)
        lon_len = 60
        if key is "DS_DIM_SIZE_CHANGED":
            lon_len = 70
        lon = ds.createDimension('lon', lon_len)

        ds.creator = "ocean observing initiative"
        if key is "DS_GLOBAL_ATT_CHANGED":
            ds.history = "first history entry; second history entry"
        else:
            ds.history = "first history entry"

        time = ds.createVariable('time','i8',('time',),fill_value=-1)
        time.standard_name = "time"
        time.long_name = "time"
        time.units = "seconds since 1970-01-01 00:00:00"
        time.calendar = "gregorian"
        time.comment = "estimated time of observation"


        latitudes = ds.createVariable('latitude','f4',('lat',))
        latitudes.standard_name = "latitude"
        if key is "DS_VAR_ATT_CHANGED":
            latitudes.long_name = "station has changed"
        else:
            latitudes.long_name = "station latitude"
        latitudes.units = "degrees_north"
        latitudes[:] = numpy.arange(10,50,0.5)

        longitudes = ds.createVariable('longitude','f4',('lon',))
        longitudes.standard_name = "longitude"
        longitudes.long_name = "station longitude"
        longitudes.units = "degrees_east"
        if key is "DS_DIM_SIZE_CHANGED":
            longitudes[:] = numpy.arange(-90,-55,0.5)
        else:
            longitudes[:] = numpy.arange(-90,-60,0.5)

        temp = ds.createVariable('temp','f4',('time','lat','lon',))
        temp.standard_name = "sea_water_temperature"
        temp.long_name = "sea water temperature"
        temp.units = "degrees C"
        # write the temperature data
        from numpy.random import uniform
        temp[0:10,:,:] = uniform(size=(10,len(ds.dimensions["lat"]),len(ds.dimensions["lon"])))

        # fill in the temporal data
        from datetime import datetime, timedelta
        from netCDF4 import num2date, date2num
        dates = [datetime(2011,12,1) + n * timedelta(hours=1) for n in range(temp.shape[0])]
        time[:] = date2num(dates,units=time.units,calendar=time.calendar)

        ds.close()

        os.close(tmp_handle)

        return ds

    def get_temp_file(self, prefix=""):
        return tempfile.mkstemp(prefix=prefix, suffix='.nc')

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
        ## Uncomment this line when the guts of "get_signature" has changed to print the new "correct" value - replace "self._ds1_sig" with the output
#        raise StandardError(signature)
        self.assertEqual(signature, self._ds1_sig)

    def test_compare_identical(self):
        tn_1,th_1,ds_1 = self._ds_list["DS1"]
        dsh_1 = self.create_tst_data_set_handler(tn_1)
        dsh_1.ds = ds_1

        tn_2,th_2,ds_2 = self._ds_list["DS1"]
        dsh_2 = self.create_tst_data_set_handler(tn_2)
        dsh_2.ds = ds_2
        
        dcr = dsh_1.compare(dsh_2)
        self.assertTrue(dcr.get_result()[0], "EQUAL")

    def test_compare_data_different(self):
        tn_1,th_1,ds_1 = self._ds_list["DS1"]
        dsh_1 = self.create_tst_data_set_handler(tn_1)
        dsh_1.ds = ds_1

        tn_2,th_2,ds_2 = self._ds_list["DS1_DUP"]
        dsh_2 = self.create_tst_data_set_handler(tn_2)
        dsh_2.ds = ds_2

        dcr = dsh_1.compare(dsh_2, data_sampling=DapExternalDataHandler.DATA_SAMPLING_FIRST_LAST)
        self.assertTrue(dcr.get_result()[0], "MOD_DATA")
    
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