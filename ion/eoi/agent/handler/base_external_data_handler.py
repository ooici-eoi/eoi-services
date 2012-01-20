#!/usr/bin/env python

__author__ = 'cmueller'

from ion.eoi.agent.handler.iface.iexternal_data_handler_controller import IExternalDataHandlerController
from zope.interface import implements

# Observatory Status Types
OBSERVATORY_ONLINE = 'ONLINE'
OBSERVATORY_OFFLINE = 'OFFLINE'

# Protocol Types
PROTOCOL_TYPE_DAP = "DAP"

class BaseExternalDataHandler():
    """ Base implementation of the External Observatory Handler"""
    implements(IExternalDataHandlerController)

    _ext_provider_res = None
    _ext_data_source_res = None
    _ext_dataset_res = None
    _signature = None
    _block_size = 10000

    def __init__(self, data_provider=None, data_source=None, ext_dset=None, *args, **kwargs):
        self._ext_provider_res = data_provider
        self._ext_data_source_res = data_source
        self._ext_dataset_res = ext_dset

        if "BLOCK_SIZE" in kwargs:
            self._block_size = int(kwargs.pop("BLOCK_SIZE"))

    # Generic, utility and helper methods

#    def calculate_decomposition(self, **kwargs):
#        return NotImplemented

    def __repr__(self):
#        return "\n>> ExternalDataProvider:\n%s\n>> DataSource:\n%s\n>>ExternalDataset\n%s" % (self._ext_provider_res, self._ext_data_source_res, self._ext_dataset_res)
        #TODO: Add printing of the ExternalDataset resource back in.
        # The above breaks tests because the id's of the nested "DatasetDescription" and "UpdateDescription" objects change by instantiation.
        # Must manually iterate the objects
        return "\n>> ExternalDataProvider:\n%s\n>> DataSource:\n%s" % (self._ext_provider_res, self._ext_data_source_res)

class ExternalDataHandlerError(Exception):
    """
    Base class for ExternalDataHandler errors.
    """
    pass

class InstantiationError(ExternalDataHandlerError):
    """
    Exception raised when an ExternalDataHandler cannot be instantiated
    """
    pass

class DataAcquisitionError(ExternalDataHandlerError):
    """
    Exception raised when there is a problem acquiring data from an external dataset.
    """
    pass
