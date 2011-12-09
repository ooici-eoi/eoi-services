#!/usr/bin/env python

__author__ = 'cmueller'

from zope.interface import Interface

class IExternalDataHandlerController(Interface):
    """Interface for all interaction with the External Observatory framework"""

    def initialize(**kwargs):
        """
        Initializes this instance of External Observatory Handler
        """
        pass

    def get_status(**kwargs):
        """
        Returns the status of the External Observatory.  The response is a member of the OBSERVATORY_* attribute set in this interface
        """
        pass

    def get_catalog(**kwargs):
        """
        Returns a catalog of assets from the External Observatory
        """
        pass

    def has_new_data(**kwargs):
        """
        Returns a boolean indicating if there is new data available from the External Observatory
        """
        pass

    def acquire_new_data(**kwargs):
        """
        """
        pass

    def acquire_historical_data(**kwargs):
        """
        """
        pass