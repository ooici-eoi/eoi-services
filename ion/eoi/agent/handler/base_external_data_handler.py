#!/usr/bin/env python

__author__ = 'cmueller'

from pyon.service.service import BaseClients
from pyon.public import IonObject
from pyon.public import log
from ion.eoi.agent.interface.iexternal_data_handler_controller import IExternalDataHandlerController
from zope.interface import implements

# Observatory Status Types
OBSERVATORY_ONLINE = 'ONLINE'
OBSERVATORY_OFFLINE = 'OFFLINE'

# Protocol Types
PROTOCOL_TYPE_DAP = "DAP"

class BaseExternalDataHandler():
    """ Base implementation of the External Observatory Handler"""
    implements(IExternalDataHandlerController)

    DATA_SAMPLING_FIRST_LAST = "FIRST_LAST"
    DATA_SAMPLING_FULL = "FULL"
    DATA_SAMPLING_SHOTGUN = "SHOTGUN"
    DATA_SAMPLING_SHOTGUN_COUNT = "SHOTGUN_COUNT"
    DATA_SAMPLING_NONE = "NONE"

    _ext_provider_res = None
    _ext_data_source_res = None
    _ext_dataset_res = None
    _dataset_desc_obj = None
    _update_desc_obj = None

    def __init__(self, data_provider=None, data_source=None, ext_dset=None, ds_desc=None, update_desc=None, *args, **kwargs):
        self._ext_provider_res = data_provider
        self._ext_data_source_res = data_source
        self._ext_dataset_res = ext_dset
        self._dataset_desc_obj = ds_desc
        self._update_desc_obj = update_desc

    def get_status(self, **kwargs):
        return NotImplemented

    def has_new_data(self, **kwargs):
        return False

    def acquire_data(self, request=None, **kwargs):
        return NotImplemented

    def acquire_new_data(self, **kwargs):
        return NotImplemented

    def get_signature(self, recalculate=False, data_sampling=DATA_SAMPLING_NONE, **kwargs):
        return NotImplemented

    def get_attributes(self, scope=None):
        return NotImplemented

    def compare(self, BaseExternalObservatoryHandler=None):
        return NotImplemented

    # Generic, utility and helper methods

    def calculate_decomposition(self, **kwargs):
        return NotImplemented

    def __repr__(self):
#        return "on=%s off=%s" % (self.OBSERVATORY_ONLINE, self.OBSERVATORY_OFFLINE)
        return "\n>> ExternalDataProvider:\n%s\n>> DataSource:\n%s\n>>ExternalDataset\n%s" % (self._ext_provider_res, self._ext_data_source_res, self._ext_dataset_res)
