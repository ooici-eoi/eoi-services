__author__ = 'timgiguere'
__author__ = 'cmueller'

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

    _dsh_list = {}
    _ds_base_sig = None
    _ten_x_ten_x_ten = None
    _twelve_x_ten_x_ten = None

    def setUp(self):
        self._ds_base_sig = ('603beb66f55d84162619a5c953d13dbcd7fb1ccb', {'dims': ('9e86329cc35148d33f94da5d9a45c4996b8ff756', {u'lat': '17b0bff9fd5da05a17e27478f72d39216546eafa', u'lon': 'f115170f91587fb723e33ac1564eb021615acfaa', u'time': '165058f1f201a8e9e455687940ce890121dd7853'}), 'gbl_atts': ('09a13128e45c6e1909c4b762bdd0d84b9736cfbe', {u'history': '70d1ff82dbc7cc7a0972f67ab75ce93a184f160f', u'creator': '3c5b4941b7b4594facec9eb943e4d3c5401d882a'}), 'vars': ('d1ef78860256fdc35dbd99df43d69a71a850f014', {u'latitude': ('fa639066e2cbff9006c7abffce0b2f2d4be74a5f', {u'units': '84f9eb7a658bfac37e07152c3ea75548fee6f512', u'long_name': '5fcccdcf1d079c4a85c92c6fe7c8d29a27e49bed', u'standard_name': '5fcccdcf1d079c4a85c92c6fe7c8d29a27e49bed'}), u'temp': ('38416f2e10419e0cf74964a1fedd511a1031b877', {u'units': '53f8a000a1f0bc4b59a178b0bcea46c9ef8ff160', u'long_name': 'b7d4eadd3b0618f8164c53f1c31df4acad5db392', u'standard_name': '8e8c717ded6a5973b95661d2f298e175c04b1624'}), u'longitude': ('4812c8a8a7814777a5bc9d8bcaa04a83e929deae', {u'units': 'ff371c19d6872ff49d09490ecbdffbe3e7af2be5', u'long_name': 'd2a773ae817d7d07c19d9e37be4e792cec37aff0', u'standard_name': 'd2a773ae817d7d07c19d9e37be4e792cec37aff0'}), u'time': ('036112bc95c35ba8ccff3ae54d77e76c881ad1af', {u'comment': 'd4bd716c721dd666ab529575d2ccaea4f0867dd9', u'_FillValue': '7984b0a0e139cabadb5afc7756d473fb34d23819', u'long_name': '714eea0f4c980736bde0065fe73f573487f08e3a', u'standard_name': '714eea0f4c980736bde0065fe73f573487f08e3a', u'units': 'd779896565174fdacafb4c96fc70455a2ac7d826', u'calendar': '4a78d3cb314a4e97cfe37eda5781f60b87f6145e'})})})

        self._ds1_repr = "\n>> ExternalDataProvider:\nNone\n>> DataSource:\nNone\n>>ExternalDataset\nNone\n***\ndataset:\n<type 'netCDF4.Dataset'>\nroot group (NETCDF4 file format):\n    creator: ocean observing initiative\n    history: first history entry\n    dimensions = ('time', 'lat', 'lon')\n    variables = ('time', 'latitude', 'longitude', 'temp')\n    groups = ()\n\ntime_var: <type 'netCDF4.Variable'>\nint64 time('time',)\n    _FillValue: -1\n    standard_name: time\n    long_name: time\n    units: seconds since 1970-01-01 00:00:00\n    calendar: gregorian\n    comment: estimated time of observation\nunlimited dimensions = ('time',)\ncurrent size = (10,)\n\ndataset_signature(sha1): 603beb66f55d84162619a5c953d13dbcd7fb1ccb\n\tdims: 9e86329cc35148d33f94da5d9a45c4996b8ff756\n\t\tlat: 17b0bff9fd5da05a17e27478f72d39216546eafa\n\t\tlon: f115170f91587fb723e33ac1564eb021615acfaa\n\t\ttime: 165058f1f201a8e9e455687940ce890121dd7853\n\tgbl_atts: 09a13128e45c6e1909c4b762bdd0d84b9736cfbe\n\t\thistory: 70d1ff82dbc7cc7a0972f67ab75ce93a184f160f\n\t\tcreator: 3c5b4941b7b4594facec9eb943e4d3c5401d882a\n\tvars: d1ef78860256fdc35dbd99df43d69a71a850f014\n\t\tlatitude: fa639066e2cbff9006c7abffce0b2f2d4be74a5f\n\t\t\tunits: 84f9eb7a658bfac37e07152c3ea75548fee6f512\n\t\t\tlong_name: 5fcccdcf1d079c4a85c92c6fe7c8d29a27e49bed\n\t\t\tstandard_name: 5fcccdcf1d079c4a85c92c6fe7c8d29a27e49bed\n\t\ttemp: 38416f2e10419e0cf74964a1fedd511a1031b877\n\t\t\tunits: 53f8a000a1f0bc4b59a178b0bcea46c9ef8ff160\n\t\t\tlong_name: b7d4eadd3b0618f8164c53f1c31df4acad5db392\n\t\t\tstandard_name: 8e8c717ded6a5973b95661d2f298e175c04b1624\n\t\tlongitude: 4812c8a8a7814777a5bc9d8bcaa04a83e929deae\n\t\t\tunits: ff371c19d6872ff49d09490ecbdffbe3e7af2be5\n\t\t\tlong_name: d2a773ae817d7d07c19d9e37be4e792cec37aff0\n\t\t\tstandard_name: d2a773ae817d7d07c19d9e37be4e792cec37aff0\n\t\ttime: 036112bc95c35ba8ccff3ae54d77e76c881ad1af\n\t\t\tcomment: d4bd716c721dd666ab529575d2ccaea4f0867dd9\n\t\t\t_FillValue: 7984b0a0e139cabadb5afc7756d473fb34d23819\n\t\t\tlong_name: 714eea0f4c980736bde0065fe73f573487f08e3a\n\t\t\tstandard_name: 714eea0f4c980736bde0065fe73f573487f08e3a\n\t\t\tunits: d779896565174fdacafb4c96fc70455a2ac7d826\n\t\t\tcalendar: 4a78d3cb314a4e97cfe37eda5781f60b87f6145e\n"

        self._dsh_list = {}
        # TODO: use these to key the creation of various test datasets
        lst = ["DS_BASE", "DS_BASE_DUP", "DS_DIM_SIZE_CHANGED", "DS_VAR_ATT_CHANGED", "DS_GLOBAL_ATT_CHANGED", "DS_ADDITIONAL_TIMES"]

        for key in lst:
            self._create_tst_data_set_handler(key=key)

    def tearDown(self):
        for key in self._dsh_list:
            os.remove(self._dsh_list[key][1])
        pass

    def _create_tst_data_set(self, tmp_handle, tmp_name, key):
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
            latitudes.long_name = "latitude has changed"
        else:
            latitudes.long_name = "latitude"
        latitudes.units = "degrees_north"
        latitudes[:] = numpy.arange(10,50,0.5)

        longitudes = ds.createVariable('longitude','f4',('lon',))
        longitudes.standard_name = "longitude"
        longitudes.long_name = "longitude"
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
        if key is "DS_ADDITIONAL_TIMES":
            temp[0:12,:,:] = uniform(size=(12,len(ds.dimensions["lat"]),len(ds.dimensions["lon"])))
        else:
            temp[0:10,:,:] = uniform(size=(10,len(ds.dimensions["lat"]),len(ds.dimensions["lon"])))

        # fill in the temporal data
        from datetime import datetime, timedelta
        from netCDF4 import num2date, date2num
        dates = [datetime(2011,12,1) + n * timedelta(hours=1) for n in range(temp.shape[0])]
        time[:] = date2num(dates,units=time.units,calendar=time.calendar)

        ds.close()

        os.close(tmp_handle)

    def _create_tst_data_set_handler(self, key="DS_BASE"):
        th,tn=tempfile.mkstemp(prefix=(key + "_"), suffix='.nc')
        self._create_tst_data_set(th,tn,key)

        dataDesc = IonObject("DapDatasetDescription", name="test", dataset_path=tn, temporal_dimension="time")

        dsh = DapExternalDataHandler(dataset_desc=dataDesc)

        self._dsh_list[key]=dsh, tn


    #### Test Methods

    @unittest.skip("Override and skip - not a service; avoids unnecessary 'setUp' cycle")
    def test_verify_service(self):
        pass

#    @unittest.skip("for now")
    def test_get_signature(self):
        dsh = self._dsh_list["DS_BASE"][0]
        signature = dsh.get_signature()
        ## Uncomment this line when the guts of "get_signature" has changed to print the new "correct" value - replace "self._ds_base_sig" with the output
#        raise StandardError(signature)
        self.assertEqual(signature, self._ds_base_sig)

#    @unittest.skip("for now")
    def test_compare_identical(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        dsh_2 = self._dsh_list["DS_BASE_DUP"][0]

        dcr = dsh_1.compare(dsh_2.get_signature())
        self.assertTrue(dcr.get_result()[0], "EQUAL")

#    @unittest.skip("for now")
    def test_compare_data_different_first_last(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]
        dsh_1._dataset_desc_obj.data_sampling = DapExternalDataHandler.DATA_SAMPLING_FIRST_LAST

        dsh_2 = self._dsh_list["DS_BASE_DUP"][0]
        dsh_2._dataset_desc_obj.data_sampling = DapExternalDataHandler.DATA_SAMPLING_FIRST_LAST

        dcr = dsh_1.compare(dsh_2.get_signature())
        self.assertTrue(dcr.get_result()[0], "MOD_DATA")

#    @unittest.skip("for now")
    def test_compare_data_different_full(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]
        dsh_1._dataset_desc_obj.data_sampling = DapExternalDataHandler.DATA_SAMPLING_FULL

        dsh_2 = self._dsh_list["DS_BASE_DUP"][0]
        dsh_2._dataset_desc_obj.data_sampling = DapExternalDataHandler.DATA_SAMPLING_FULL

        dcr = dsh_1.compare(dsh_2.get_signature())
        self.assertTrue(dcr.get_result()[0], "MOD_DATA")

#    @unittest.skip("for now")
    def test_compare_global_attribute_changed(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        dsh_2 = self._dsh_list["DS_GLOBAL_ATT_CHANGED"][0]

        dcr = dsh_1.compare(dsh_2.get_signature())
        self.assertTrue(dcr.get_result()[0], "MOD_GBL_ATTS")

#    @unittest.skip("for now")
    def test_compare_dim_size_changed(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        dsh_2 = self._dsh_list["DS_DIM_SIZE_CHANGED"][0]

        dcr = dsh_1.compare(dsh_2.get_signature())
        self.assertTrue(dcr.get_result()[0], "MOD_DIMS")

#    @unittest.skip("for now")
    def test_get_attributes_global(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        atts = dsh_1.get_attributes()
        self.assertEqual(atts, {"creator": "ocean observing initiative", "history": "first history entry"})

#    @unittest.skip("for now")
    def test_get_attributes_variable(self, scope="temp"):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        atts = dsh_1.get_attributes(scope=scope)
        self.assertEqual(atts, {"standard_name": "sea_water_temperature", "long_name": "sea water temperature", "units": "degrees C"})

#    @unittest.skip("for now")
    def test_constructor(self):
        dataDesc = IonObject("DapDatasetDescription", name="test")
        dataDesc.dataset_path = self._dsh_list["DS_BASE"][1]
        dataDesc.temporal_dimension = "time"

        dsh = DapExternalDataHandler(dataset_desc=dataDesc)
        self.assertTrue(type(dsh), DapExternalDataHandler)

        dsrc = IonObject("DataSource", name="test")
        dsrc.base_data_url = ""

        dsh1 = DapExternalDataHandler(data_source=dsrc, dataset_desc=dataDesc)
        self.assertTrue(type(dsh1), DapExternalDataHandler)

#    @unittest.skip("for now")
    def test_constructor_exception(self):
        self.assertRaises(Exception, DapExternalDataHandler, dataset_desc=None)

#    @unittest.skip("for now")
    def test_base_handler_repr(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        self.assertEqual(str(dsh_1), self._ds1_repr)
        
#    @unittest.skip("for now")
    def test_has_new_data_false(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        upd_desc = IonObject("UpdateDescription", name="test")
        upd_desc.last_signature = self._ds_base_sig

        dsh_1._update_desc_obj = upd_desc

        self.assertFalse(dsh_1.has_new_data())

#    @unittest.skip("for now")
    def test_has_new_data_true(self):
        dsh_1 = self._dsh_list["DS_ADDITIONAL_TIMES"][0]

        upd_desc = IonObject("UpdateDescription", name="test")
        upd_desc.last_signature = self._ds_base_sig

        dsh_1._update_desc_obj = upd_desc

        self.assertTrue(dsh_1.has_new_data())

#    @unittest.skip("for now")
    def test_acquire_data_multidim(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        req = IonObject("PydapVarDataRequest", name="temp", slice=(slice(0),slice(0,10),slice(0,10)))

        name, data, typecode, dims, attrs = dsh_1.acquire_data(request=req)
#        raise StandardError(str(name) + "\n" + str(data) + "\n" + str(typecode) + "\n" + str(dims) + "\n" + str(attrs))

        self.assertEqual(name, "temp")
        self.assertEqual(typecode, 'f')
        self.assertEqual(dims, (u'time', u'lat', u'lon'))
        self.assertEqual(attrs, {u'units': u'degrees C', u'long_name': u'sea water temperature', u'standard_name': u'sea_water_temperature'})
        import numpy
        arr = numpy.asarray(data).flat.copy()
        lt_1 = arr<1
        gt_0 = arr>0
#        self.assertEqual(type(data), arrayterator.Arrayterator)
        self.assertTrue(lt_1.min(), lt_1.max())
        self.assertTrue(gt_0.min(), gt_0.max())

#    @unittest.skip("for now")
    def test_acquire_data_onedim(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        req = IonObject("PydapVarDataRequest", name="longitude", slice=(slice(0,10)))

        name, data, typecode, dims, attrs = dsh_1.acquire_data(request=req)
#        raise StandardError(str(name) + "\n" + str(data) + "\n" + str(typecode) + "\n" + str(dims) + "\n" + str(attrs))
        self.assertEqual(name, "longitude")
        self.assertEqual(typecode, 'f')
        self.assertEqual(dims, (u'lon',))
        self.assertEqual(attrs, {u'units': u'degrees_east', u'long_name': u'longitude', u'standard_name': u'longitude'})
        import numpy
        arr = numpy.asarray(data).flat.copy()
        self.assertTrue(numpy.array_equiv(arr, numpy.arange(-90,-85,0.5,dtype='float32')))

#    @unittest.skip("for now")
    def test_acquire_data_with_dim_no_var(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        req = IonObject("PydapVarDataRequest", name="lon", slice=(slice(0,10)))

        name, data, typecode, dims, attrs = dsh_1.acquire_data(request=req)
#        raise StandardError(str(name) + "\n" + str(data) + "\n" + str(typecode) + "\n" + str(dims) + "\n" + str(attrs))
        self.assertEqual(name, "lon")
        self.assertEqual(typecode, 'l')
        self.assertEqual(dims, ('lon',))
        self.assertEqual(attrs, {})
        import numpy
        arr = numpy.asarray(data).flat.copy()
        self.assertTrue(numpy.array_equiv(arr, numpy.arange(0,10)))


    @unittest.skip("acquire_data should throw an exception in this case")
    def test_acquire_data_with_no_dim_or_var(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        req = IonObject("PydapVarDataRequest", name="no_var_or_dim_with_this_name", slice=(slice(0,10)))

        name, data, typecode, dims, attrs = dsh_1.acquire_data(request=req)
        raise StandardError(str(name) + "\n" + str(data) + "\n" + str(typecode) + "\n" + str(dims) + "\n" + str(attrs))



if __name__ == '__main__':
    unittest.main()