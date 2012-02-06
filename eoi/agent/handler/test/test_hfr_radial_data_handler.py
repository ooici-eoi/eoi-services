__author__ = 'timgiguere'

import os
from nose.plugins.attrib import attr
from eoi.agent.handler.hfr_radial_data_handler import HfrRadialDataHandler
from eoi.agent.handler.base_external_data_handler import DataAcquisitionError, InstantiationError
from pyon.util.unit_test import PyonTestCase
from interface.objects import ExternalDataRequest, DatasetDescriptionDataSamplingEnum, ExternalDataset, DatasetDescription, UpdateDescription, ContactInformation, CompareResultEnum
import unittest
import numpy
import tempfile


@attr('UNIT', group='eoi1')
class TestHfrRadialDataHandler(PyonTestCase):

    def setUp(self):

        file1 = self._create_tst_data_set('one')

        ext_ds = ExternalDataset(name="test1", dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        ext_ds.dataset_description.parameters["dataset_path"] = file1
        ext_ds.dataset_description.parameters["temporal_dimension"] = 'time'

        self._hfr_data_handler_1 = HfrRadialDataHandler(data_source=file1, ext_dataset=ext_ds)

        file2 = self._create_tst_data_set('two')

        ext_ds2 = ExternalDataset(name="test2", dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        ext_ds2.dataset_description.parameters["dataset_path"] = file2
        ext_ds2.dataset_description.parameters["temporal_dimension"] = 'time'
        self._hfr_data_handler_2 = HfrRadialDataHandler(data_source=file2, ext_dataset=ext_ds2)

        file3 = self._create_tst_data_set('three')

        ext_ds3 = ExternalDataset(name="test3", dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        ext_ds3.dataset_description.parameters["dataset_path"] = file3
        ext_ds3.dataset_description.parameters["temporal_dimension"] = 'time'

        self._hfr_data_handler_3 = HfrRadialDataHandler(data_source=file3, ext_dataset=ext_ds3)

        file4 = self._create_tst_data_set('four')

        ext_ds4 = ExternalDataset(name="test4", dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        ext_ds4.dataset_description.parameters["dataset_path"] = file4
        ext_ds4.dataset_description.parameters["temporal_dimension"] = 'time'
        self._hfr_data_handler_4 = HfrRadialDataHandler(data_source=file4, ext_dataset=ext_ds4)

        file5 = self._create_tst_data_set('five')

        ext_ds5 = ExternalDataset(name="test5", dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        ext_ds5.dataset_description.parameters["dataset_path"] = file5
        ext_ds5.dataset_description.parameters["temporal_dimension"] = 'time'
        self._hfr_data_handler_5 = HfrRadialDataHandler(data_source=file5, ext_dataset=ext_ds5)

        file6 = self._create_tst_data_set('six')

        ext_ds6 = ExternalDataset(name="test6", dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        ext_ds6.dataset_description.parameters["dataset_path"] = file6
        ext_ds6.dataset_description.parameters["temporal_dimension"] = 'time'
        self._hfr_data_handler_6 = HfrRadialDataHandler(data_source=file6, ext_dataset=ext_ds6)

        pass

    def tearDown(self):
        self._hfr_data_handler_1 = None
        self._hfr_data_handler_2 = None
        self._hfr_data_handler_3 = None
        self._hfr_data_handler_4 = None
        pass

    def _create_tst_data_set(self, key):
        #attributes contents 1, data contents 1
        temp_file_one = '%CTF: 1.00 \n'\
                              '%FileType: LLUV rdls "RadialMap" \n'\
                              '%LLUVSpec: 1.14  2010 07 18 \n'\
                              '%UUID: 9D847DC7-BCD4-4624-85D3-7F00A1294F71 \n'\
                              '%Manufacturer: CODAR Ocean Sensors. SeaSonde \n'\
                              '%Site: SEAB "" \n'\
                              '%TimeStamp: 2011 08 24  16 00 00 \n'\
                              '%TimeZone: "UTC" +0.000 0 "Atlantic/Reykjavik" \n'\
                              '%TableType: LLUV RDL9 \n'\
                              '%TableColumns: 18 \n'\
                              '%TableColumnTypes: LOND LATD VELU VELV VFLG ESPC ETMP MAXV MINV ERSC ERTC XDST YDST RNGE BEAR VELO HEAD SPRC \n'\
                              '%TableRows: 595 \n'\
                              '%TableStart: \n'\
                              '%%   Longitude   Latitude    U comp   V comp  VectorFlag    Spatial    Temporal     Velocity    Velocity  Spatial  Temporal X Distance  Y Distance   Range   Bearing   Velocity  Direction   Spectra \n'\
                              '%%     (deg)       (deg)     (cm/s)   (cm/s)  (GridCode)    Quality     Quality     Maximum     Minimum    Count    Count      (km)        (km)       (km)    (True)    (cm/s)     (True)    RngCell \n'\
                              '-73.9711577  40.3898353    0.261    7.464          0      11.842      11.842      -7.469     -33.607       3        3       0.1055      3.0206    3.0224     2.0     -7.469     182.0         1 \n'\
                              '-73.9680618  40.3896489    3.832   31.195          0     999.000       1.862     -31.429     -31.429       1        4       0.3683      2.9999    3.0224     7.0    -31.429     187.0         1 \n'\
                              '-73.9649991  40.3892569    3.932   18.491          0      18.515      10.270      -9.646     -28.162       2        6       0.6284      2.9564    3.0224    12.0    -18.904     192.0         1 \n'\
                              '-73.9619927  40.3886621    3.459   11.308          0      16.966       8.378      12.135     -29.251       3        7       0.8837      2.8903    3.0224    17.0    -11.825     197.0         1 \n'\
                              '-73.9590656  40.3878692    6.574   16.265          0       4.485       8.910     -11.825     -24.894       5        5       1.1322      2.8023    3.0224    22.0    -17.543     202.0         1 \n'\
                              '-73.9562401  40.3868841    9.204   18.055          0       1.903       6.439     -18.359     -22.716       2        6       1.3721      2.6930    3.0224    27.0    -20.266     207.0         1 \n'\
                              '%TableEnd: \n'

        #attributes contents 2, data contents 1
        temp_file_two = '%CTF: 1.00 \n'\
                              '%FileType: LLUV rdls "RadialMap" \n'\
                              '%LLUVSpec: 1.14  2010 07 18 \n'\
                              '%UUID: E88CB18F-6DB6-4656-8596-5E2EF58F2116 \n'\
                              '%Manufacturer: CODAR Ocean Sensors. SeaSonde \n'\
                              '%Site: WILD "" \n'\
                              '%TimeStamp: 2011 12 19  14 00 00 \n'\
                              '%TimeZone: "UTC" +0.000 0 "Atlantic/Reykjavik" \n'\
                              '%TableType: LLUV RDL9 \n'\
                              '%TableColumns: 18 \n'\
                              '%TableColumnTypes: LOND LATD VELU VELV VFLG ESPC ETMP MAXV MINV ERSC ERTC XDST YDST RNGE BEAR VELO HEAD SPRC \n'\
                              '%TableRows: 595 \n'\
                              '%TableStart: \n'\
                              '%%   Longitude   Latitude    U comp   V comp  VectorFlag    Spatial    Temporal     Velocity    Velocity  Spatial  Temporal X Distance  Y Distance   Range   Bearing   Velocity  Direction   Spectra \n'\
                              '%%     (deg)       (deg)     (cm/s)   (cm/s)  (GridCode)    Quality     Quality     Maximum     Minimum    Count    Count      (km)        (km)       (km)    (True)    (cm/s)     (True)    RngCell \n'\
                              '-73.9711577  40.3898353    0.261    7.464          0      11.842      11.842      -7.469     -33.607       3        3       0.1055      3.0206    3.0224     2.0     -7.469     182.0         1 \n'\
                              '-73.9680618  40.3896489    3.832   31.195          0     999.000       1.862     -31.429     -31.429       1        4       0.3683      2.9999    3.0224     7.0    -31.429     187.0         1 \n'\
                              '-73.9649991  40.3892569    3.932   18.491          0      18.515      10.270      -9.646     -28.162       2        6       0.6284      2.9564    3.0224    12.0    -18.904     192.0         1 \n'\
                              '-73.9619927  40.3886621    3.459   11.308          0      16.966       8.378      12.135     -29.251       3        7       0.8837      2.8903    3.0224    17.0    -11.825     197.0         1 \n'\
                              '-73.9590656  40.3878692    6.574   16.265          0       4.485       8.910     -11.825     -24.894       5        5       1.1322      2.8023    3.0224    22.0    -17.543     202.0         1 \n'\
                              '-73.9562401  40.3868841    9.204   18.055          0       1.903       6.439     -18.359     -22.716       2        6       1.3721      2.6930    3.0224    27.0    -20.266     207.0         1 \n'\
                              '%TableEnd: \n'

        #attributes contents 1, data contents 2
        temp_file_three = '%CTF: 1.00 \n'\
                                '%FileType: LLUV rdls "RadialMap" \n'\
                                '%LLUVSpec: 1.14  2010 07 18 \n'\
                                '%UUID: 9D847DC7-BCD4-4624-85D3-7F00A1294F71 \n'\
                                '%Manufacturer: CODAR Ocean Sensors. SeaSonde \n'\
                                '%Site: SEAB "" \n'\
                                '%TimeStamp: 2011 08 24  16 00 00 \n'\
                                '%TimeZone: "UTC" +0.000 0 "Atlantic/Reykjavik" \n'\
                                '%TableType: LLUV RDL9 \n'\
                                '%TableColumns: 18 \n'\
                                '%TableColumnTypes: LOND LATD VELU VELV VFLG ESPC ETMP MAXV MINV ERSC ERTC XDST YDST RNGE BEAR VELO HEAD SPRC \n'\
                                '%TableRows: 595 \n'\
                                '%TableStart: \n'\
                                '%%   Longitude   Latitude    U comp   V comp  VectorFlag    Spatial    Temporal     Velocity    Velocity  Spatial  Temporal X Distance  Y Distance   Range   Bearing   Velocity  Direction   Spectra \n'\
                                '%%     (deg)       (deg)     (cm/s)   (cm/s)  (GridCode)    Quality     Quality     Maximum     Minimum    Count    Count      (km)        (km)       (km)    (True)    (cm/s)     (True)    RngCell \n'\
                                '-74.8203594  38.9530647   18.856    3.994          0     999.000     999.000     -19.274     -19.274       1        2       5.7015      1.2119    5.8289    78.0    -19.274     258.0         1 \n'\
                                '-74.8193954  38.9485464   12.484    1.524          0       7.871       6.174      -3.447     -25.766       3        3       5.7855      0.7104    5.8289    83.0    -12.577     263.0         1 \n'\
                                '-74.8189395  38.9439798   22.507    0.769          0       2.434      18.800     -17.651     -22.520       2        5       5.8253      0.2034    5.8289    88.0    -22.520     268.0         1 \n'\
                                '-74.8189950  38.9393994   24.109   -1.281          0     999.000       9.770     -24.143     -24.143       1        5       5.8209     -0.3051    5.8289    93.0    -24.143     273.0         1 \n'\
                                '-74.8195614  38.9348403   13.458   -1.901          0      19.713       6.498       1.422     -43.618       5        5       5.7722     -0.8112    5.8289    98.0    -13.592     278.0         1 \n'\
                                '-74.8206343  38.9303370   12.452   -2.884          0       5.776      11.331     -12.782     -25.766       3        5       5.6795     -1.3112    5.8289   103.0    -12.782     283.0         1 \n'\
                                '%TableEnd: \n'

        #attributes contents 2, data contents 2
        temp_file_four = '%CTF: 1.00 \n'\
                               '%FileType: LLUV rdls "RadialMap" \n'\
                               '%LLUVSpec: 1.14  2010 07 18 \n'\
                               '%UUID: E88CB18F-6DB6-4656-8596-5E2EF58F2116 \n'\
                               '%Manufacturer: CODAR Ocean Sensors. SeaSonde \n'\
                               '%Site: WILD "" \n'\
                               '%TimeStamp: 2011 12 19  14 00 00 \n'\
                               '%TimeZone: "UTC" +0.000 0 "Atlantic/Reykjavik" \n'\
                               '%TableType: LLUV RDL9 \n'\
                               '%TableColumns: 18 \n'\
                               '%TableColumnTypes: LOND LATD VELU VELV VFLG ESPC ETMP MAXV MINV ERSC ERTC XDST YDST RNGE BEAR VELO HEAD SPRC \n'\
                               '%TableRows: 595 \n'\
                               '%TableStart: \n'\
                               '%%   Longitude   Latitude    U comp   V comp  VectorFlag    Spatial    Temporal     Velocity    Velocity  Spatial  Temporal X Distance  Y Distance   Range   Bearing   Velocity  Direction   Spectra \n'\
                               '%%     (deg)       (deg)     (cm/s)   (cm/s)  (GridCode)    Quality     Quality     Maximum     Minimum    Count    Count      (km)        (km)       (km)    (True)    (cm/s)     (True)    RngCell \n'\
                               '-74.8203594  38.9530647   18.856    3.994          0     999.000     999.000     -19.274     -19.274       1        2       5.7015      1.2119    5.8289    78.0    -19.274     258.0         1 \n'\
                               '-74.8193954  38.9485464   12.484    1.524          0       7.871       6.174      -3.447     -25.766       3        3       5.7855      0.7104    5.8289    83.0    -12.577     263.0         1 \n'\
                               '-74.8189395  38.9439798   22.507    0.769          0       2.434      18.800     -17.651     -22.520       2        5       5.8253      0.2034    5.8289    88.0    -22.520     268.0         1 \n'\
                               '-74.8189950  38.9393994   24.109   -1.281          0     999.000       9.770     -24.143     -24.143       1        5       5.8209     -0.3051    5.8289    93.0    -24.143     273.0         1 \n'\
                               '-74.8195614  38.9348403   13.458   -1.901          0      19.713       6.498       1.422     -43.618       5        5       5.7722     -0.8112    5.8289    98.0    -13.592     278.0         1 \n'\
                               '-74.8206343  38.9303370   12.452   -2.884          0       5.776      11.331     -12.782     -25.766       3        5       5.6795     -1.3112    5.8289   103.0    -12.782     283.0         1 \n'\
                               '%TableEnd: \n'

        temp_file_five = '%CTF: 1.00 \n'\
                         '%FileType: LLUV rdls "RadialMap" \n'\
                         '%LLUVSpec: 1.14  2010 07 18 \n'\
                         '%UUID: E88CB18F-6DB6-4656-8596-5E2EF58F2116 \n'\
                         '%Manufacturer: CODAR Ocean Sensors. SeaSonde \n'\
                         '%Site: WILD "" \n'\
                         '%TimeStamp: 2011 12 19  14 00 00 \n'\
                         '%TableType: LLUV RDL9 \n'\
                         '%TableColumns: 18 \n'\
                         '%TableColumnTypes: LOND LATD VELU VELV VFLG ESPC ETMP MAXV MINV ERSC ERTC XDST YDST RNGE BEAR VELO HEAD SPRC \n'\
                         '%TableRows: 595 \n'\
                         '%TableStart: \n'\
                         '%%   Longitude   Latitude    U comp   V comp  VectorFlag    Spatial    Temporal     Velocity    Velocity  Spatial  Temporal X Distance  Y Distance   Range   Bearing   Velocity  Direction   Spectra \n'\
                         '%%     (deg)       (deg)     (cm/s)   (cm/s)  (GridCode)    Quality     Quality     Maximum     Minimum    Count    Count      (km)        (km)       (km)    (True)    (cm/s)     (True)    RngCell \n'\
                         '-74.8203594  38.9530647   18.856    3.994          0     999.000     999.000     -19.274     -19.274       1        2       5.7015      1.2119    5.8289    78.0    -19.274     258.0         1 \n'\
                         '-74.8193954  38.9485464   12.484    1.524          0       7.871       6.174      -3.447     -25.766       3        3       5.7855      0.7104    5.8289    83.0    -12.577     263.0         1 \n'\
                         '-74.8189395  38.9439798   22.507    0.769          0       2.434      18.800     -17.651     -22.520       2        5       5.8253      0.2034    5.8289    88.0    -22.520     268.0         1 \n'\
                         '-74.8189950  38.9393994   24.109   -1.281          0     999.000       9.770     -24.143     -24.143       1        5       5.8209     -0.3051    5.8289    93.0    -24.143     273.0         1 \n'\
                         '-74.8195614  38.9348403   13.458   -1.901          0      19.713       6.498       1.422     -43.618       5        5       5.7722     -0.8112    5.8289    98.0    -13.592     278.0         1 \n'\
                         '-74.8206343  38.9303370   12.452   -2.884          0       5.776      11.331     -12.782     -25.766       3        5       5.6795     -1.3112    5.8289   103.0    -12.782     283.0         1 \n'\
                         '%TableEnd: \n'

        temp_file_six = '%CTF: 1.00 \n'\
                        '%FileType: LLUV rdls "RadialMap" \n'\
                        '%LLUVSpec: 1.14  2010 07 18 \n'\
                        '%UUID: 9D847DC7-BCD4-4624-85D3-7F00A1294F71 \n'\
                        '%Manufacturer: CODAR Ocean Sensors. SeaSonde \n'\
                        '%Site: SEAB "" \n'\
                        '%TimeStamp: 2011 08 24  16 00 00 \n'\
                        '%TimeZone: "UTC" +0.000 0 "Atlantic/Reykjavik" \n'\
                        '%TableType: LLUV RDL9 \n'\
                        '%TableColumns: 18 \n'\
                        '%TableColumnTypes: LOND LATD VELU VELV VFLG ESPC ETMP MAXV MINV ERSC ERTC XDST YDST RNGE BEAR VELO HEAD SPRC \n'\
                        '%TableRows: 595 \n'\
                        '%TableStart: \n'\
                        '%%   Longitude   Latitude    U comp   V comp  VectorFlag    Spatial    Temporal     Velocity    Velocity  Spatial  Temporal X Distance  Y Distance   Range   Bearing   Velocity  Direction   Spectra \n'\
                        '%%     (rad)       (rad)     (cm/s)   (cm/s)  (GridCode)    Quality     Quality     Maximum     Minimum    Count    Count      (km)        (km)       (km)    (True)    (cm/s)     (True)    RngCell \n'\
                        '-73.9711577  40.3898353    0.261    7.464          0      11.842      11.842      -7.469     -33.607       3        3       0.1055      3.0206    3.0224     2.0     -7.469     182.0         1 \n'\
                        '-73.9680618  40.3896489    3.832   31.195          0     999.000       1.862     -31.429     -31.429       1        4       0.3683      2.9999    3.0224     7.0    -31.429     187.0         1 \n'\
                        '-73.9649991  40.3892569    3.932   18.491          0      18.515      10.270      -9.646     -28.162       2        6       0.6284      2.9564    3.0224    12.0    -18.904     192.0         1 \n'\
                        '-73.9619927  40.3886621    3.459   11.308          0      16.966       8.378      12.135     -29.251       3        7       0.8837      2.8903    3.0224    17.0    -11.825     197.0         1 \n'\
                        '-73.9590656  40.3878692    6.574   16.265          0       4.485       8.910     -11.825     -24.894       5        5       1.1322      2.8023    3.0224    22.0    -17.543     202.0         1 \n'\
                        '-73.9562401  40.3868841    9.204   18.055          0       1.903       6.439     -18.359     -22.716       2        6       1.3721      2.6930    3.0224    27.0    -20.266     207.0         1 \n'\
                        '%TableEnd: \n'

        th, tn = tempfile.mkstemp(prefix=(key + "_"), suffix='.ruv')
        result = tn
        tempobj = os.fdopen(th, 'w')

        if key == 'one':
            tempobj.write(temp_file_one)
        elif key == 'two':
            tempobj.write(temp_file_two)
        elif key == 'three':
            tempobj.write(temp_file_three)
        elif key == 'four':
            tempobj.write(temp_file_four)
        elif key == 'five':
            tempobj.write(temp_file_five)
        elif key == 'six':
            tempobj.write(temp_file_six)

        tempobj.close()
        return result

        pass

    @unittest.skip("Not implemented yet")
    def test_get_attributes(self):
        attributes = self._hfr_data_handler_1.get_attributes()
        #make sure attributes match up here?

    @unittest.skip("Not implemented yet")
    def test_get_attribute(self):
        attributes = self._hfr_data_handler_1.get_attributes()
        for key in attributes.keys():
            attr_value = self._hfr_data_handler_1.get_attribute(key)

    @unittest.skip("Not implemented yet")
    def test_get_attribute_not_found(self):
        attr_value = self._hfr_data_handler_1.get_attribute('notfound')
        self.assertEqual(attr_value, '')

    @unittest.skip("Not implemented yet")
    def test_get_variables(self):
        variables = self._hfr_data_handler_1.get_variables()
        #make sure variables match up here?

    @unittest.skip("Not implemented yet")
    def test_acquire_data(self):
        data_iter = self._hfr_data_handler_1.acquire_data()
        for vn, slice_, rng, data in data_iter:
            self.assertTrue(isinstance(slice_, tuple))
            self.assertTrue(isinstance(rng, tuple))
            self.assertTrue(isinstance(data, numpy.ndarray))

    @unittest.skip("Not implemented yet")
    def test_acquire_data_one_variable(self):
        data_iter = self._hfr_data_handler_1.acquire_data(var_name='LOND')
        for vn, slice_, rng, data in data_iter:
            self.assertTrue(isinstance(slice_, tuple))
            self.assertTrue(isinstance(rng, tuple))
            self.assertTrue(isinstance(data, numpy.ndarray))

    @unittest.skip("Not implemented yet")
    def test_get_signature(self):
        self._hfr_data_handler_1._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.NONE
        signature = self._hfr_data_handler_1.get_signature(False)
        self.assertTrue(not len(signature) == 0)

    @unittest.skip("Not implemented yet")
    def test_get_signature_no_recalculate(self):
        self._hfr_data_handler_1._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.NONE
        signature1 = self._hfr_data_handler_1.get_signature(False) #initialize the signature
        signature2 = self._hfr_data_handler_1.get_signature(False) # make sure you get the same one back
        self.assertEqual(signature1, signature2)

    @unittest.skip("Not implemented yet")
    def test_get_signature_full(self):
        self._hfr_data_handler_1._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL
        signature = self._hfr_data_handler_1.get_signature(False)
        self.assertTrue(not len(signature) == 0)

    @unittest.skip("Not implemented yet")
    def test_get_signature_first_last(self):
        self._hfr_data_handler_1._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FIRST_LAST
        signature = self._hfr_data_handler_1.get_signature(False)
        self.assertTrue(not len(signature) == 0)

    @unittest.skip("Not implemented yet")
    def test_get_signature_shotgun(self):
        self._hfr_data_handler_1._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.SHOTGUN
        signature = self._hfr_data_handler_1.get_signature(False)
        self.assertTrue(not len(signature) == 0)

    @unittest.skip("Not implemented yet")
    def test_compare_equal_full(self):
        self._hfr_data_handler_1._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL
        signature = self._hfr_data_handler_1.get_signature(False)
        for x in self._hfr_data_handler_1.compare(signature):
            self.assertEqual(x.difference, CompareResultEnum.EQUAL)

    @unittest.skip("Not implemented yet")
    def test_compare_mod_gatt_full(self):
        self._hfr_data_handler_1._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL
        signature1 = self._hfr_data_handler_1.get_signature(False)
        self._hfr_data_handler_2._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL
        for x in self._hfr_data_handler_2.compare(signature1):
            self.assertEqual(x.difference, CompareResultEnum.MOD_GATT)

    @unittest.skip("Not implemented yet")
    def test_compare_mod_gatt_first_last(self):
        self._hfr_data_handler_1._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FIRST_LAST
        signature1 = self._hfr_data_handler_1.get_signature(False)
        self._hfr_data_handler_2._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FIRST_LAST
        for x in self._hfr_data_handler_2.compare(signature1):
            self.assertEqual(x.difference, CompareResultEnum.MOD_GATT)

    @unittest.skip('Not implemented yet')
    def test_compare_mod_gatt_shotgun(self):
        self._hfr_data_handler_1._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.SHOTGUN
        signature1 = self._hfr_data_handler_1.get_signature(False)
        self._hfr_data_handler_2._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.SHOTGUN
        for x in self._hfr_data_handler_2.compare(signature1):
            self.assertEqual(x.difference, CompareResultEnum.MOD_GATT)

    @unittest.skip("Not implemented yet")
    def test_compare_new_gatt_full(self):
        self._hfr_data_handler_4._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL
        signature1 = self._hfr_data_handler_4.get_signature(False)
        self._hfr_data_handler_5._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL
        for x in self._hfr_data_handler_5.compare(signature1):
            self.assertEqual(x.difference, CompareResultEnum.NEW_GATT)

    @unittest.skip("Not implemented yet")
    def test_compare_new_gatt_first_last(self):
        self._hfr_data_handler_4._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FIRST_LAST
        signature1 = self._hfr_data_handler_4.get_signature(False)
        self._hfr_data_handler_5._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FIRST_LAST
        for x in self._hfr_data_handler_5.compare(signature1):
            self.assertEqual(x.difference, CompareResultEnum.NEW_GATT)

    @unittest.skip('Not implemented yet')
    def test_compare_new_gatt_shotgun(self):
        self._hfr_data_handler_4._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.SHOTGUN
        signature1 = self._hfr_data_handler_4.get_signature(False)
        self._hfr_data_handler_5._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.SHOTGUN
        for x in self._hfr_data_handler_5.compare(signature1):
            self.assertEqual(x.difference, CompareResultEnum.NEW_GATT)

    def test_compare_mod_varatt_full(self):
        print 'test_compare_mod_varatt_full'
        self._hfr_data_handler_1._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL
        signature1 = self._hfr_data_handler_1.get_signature(False)
        #self._hfr_data_handler_6._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL

        #for x in self._hfr_data_handler_6.compare(signature1):
        #    self.assertEqual(x.difference, CompareResultEnum.MOD_VARATT)
        #print self._hfr_data_handler_6.get_signature(False)
        for x in self._hfr_data_handler_1.get_variables():
            print x.long_name, x.units

        for x in self._hfr_data_handler_6.get_variables():
            print x.long_name, x.units

    @unittest.skip("Not implemented yet")
    def test_compare_mod_varatt_first_last(self):
        self._hfr_data_handler_1._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FIRST_LAST
        signature1 = self._hfr_data_handler_1.get_signature(False)
        self._hfr_data_handler_6._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FIRST_LAST
        for x in self._hfr_data_handler_6.compare(signature1):
            self.assertEqual(x.difference, CompareResultEnum.MOD_VARATT)

    @unittest.skip('Not implemented yet')
    def test_compare_new_gatt_shotgun(self):
        self._hfr_data_handler_4._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.SHOTGUN
        signature1 = self._hfr_data_handler_4.get_signature(False)
        self._hfr_data_handler_5._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.SHOTGUN
        for x in self._hfr_data_handler_5.compare(signature1):
            self.assertEqual(x.difference, CompareResultEnum.NEW_GATT)

    @unittest.skip("Not implemented yet")
    def test_compare_equal_first_last(self):
        self._hfr_data_handler_1._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL
        signature = self._hfr_data_handler_1.get_signature(False)
        for x in self._hfr_data_handler_1.compare(signature):
            self.assertEqual(x.difference, CompareResultEnum.EQUAL)

    @unittest.skip("Not implemented yet")
    def test_compare_not_equal_first_last(self):
        self._hfr_data_handler_1._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL
        signature1 = self._hfr_data_handler_1.get_signature(False)
        for x in self._hfr_data_handler_2.compare(signature1):
            self.assertNotEqual(x.difference, CompareResultEnum.EQUAL)
