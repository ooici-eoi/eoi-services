__author__ = 'cmueller'

from nose.plugins.attrib import attr
from pyon.util.int_test import IonIntegrationTestCase
from ion.eoi.agent.data_acquisition_management_service_Placeholder import *
import unittest

@attr('INT', group='eoi')
class TestIntDapExternalDataHandler(IonIntegrationTestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    #### Tests
#    @unittest.skip("for now")
    def test_get_signature(self):
        # TODO: Replace this placeholder with appropriate service call(s)
        damsP = DataAcquisitionManagementServicePlaceholder()
        dsh = damsP.get_data_handler(ds_id=HFR)

        signature = dsh.get_signature()

#        raise StandardError(len(str(signature)))

        self.assertTrue(len(str(signature)) == 4562)


#    @unittest.skip("for now")
    def test_has_new_data_false(self):
        # TODO: Replace this placeholder with appropriate service call(s)
        damsP = DataAcquisitionManagementServicePlaceholder()
        dsh = damsP.get_data_handler(ds_id=HFR)

        signature = dsh.get_signature()

        upd_desc = IonObject("UpdateDescription")
        upd_desc.last_signature = signature

        dsh._update_desc_obj = upd_desc

        self.assertFalse(dsh.has_new_data())

#    @unittest.skip("for now")
    def test_has_new_data_initial(self):
        # TODO: Replace this placeholder with appropriate service call(s)
        damsP = DataAcquisitionManagementServicePlaceholder()
        dsh = damsP.get_data_handler(ds_id=HFR)

        self.assertTrue(dsh.has_new_data())

