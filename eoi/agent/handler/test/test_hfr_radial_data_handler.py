__author__ = 'timgiguere'

import os
from nose.plugins.attrib import attr
from eoi.agent.handler.hfr_radial_data_handler import HfrRadialDataHandler
from eoi.agent.handler.base_external_data_handler import DataAcquisitionError, InstantiationError
from pyon.util.unit_test import PyonTestCase
from interface.objects import ExternalDataRequest
import unittest
import numpy


@attr('UNIT', group='eoi1')
class TestHfrRadialDataHandler(PyonTestCase):

    _hfr_data_handler = None

    def setUp(self):
        self._hfr_data_handler = HfrRadialDataHandler(data_source='test_data/RDLi_SEAB_2011_08_24_1600.ruv')
        self._hfr_data_handler2 = HfrRadialDataHandler(data_source='test_data/RDLi_WILD_2011_12_19_1400.ruv')
        pass

    def tearDown(self):
        self._hfr_data_handler = None
        pass

    def test_get_attributes(self):
        attributes = self._hfr_data_handler.get_attributes()
        #make sure attributes match up here?

    def test_get_attribute(self):
        attributes = self._hfr_data_handler.get_attributes()
        for key in attributes.keys():
            attr_value = self._hfr_data_handler.get_attribute(key)

    def test_get_variables(self):
        variables = self._hfr_data_handler.get_variables()
        #make sure variables match up here?

    def test_acquire_data(self):
        data_iter = self._hfr_data_handler.acquire_data()
        for vn, slice_, rng, data in data_iter:
            self.assertTrue(isinstance(slice_, tuple))
            self.assertTrue(isinstance(rng, tuple))
            self.assertTrue(isinstance(data, numpy.ndarray))

    def test_get_signature(self):
        signature = self._hfr_data_handler.get_signature(False)
        self.assertTrue(not len(signature) == 0)

    def test_compare_equal(self):
        signature = self._hfr_data_handler.get_signature(False)
        self._hfr_data_handler.compare(signature)

    def test_compare_not_equal(self):
        signature1 = self._hfr_data_handler.get_signature(False)
        self._hfr_data_handler2.compare(signature1)
