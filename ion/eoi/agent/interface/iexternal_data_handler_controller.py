#!/usr/bin/env python

__author__ = 'cmueller'

from zope.interface import Interface

class IExternalDataHandlerController(Interface):
    """Interface for all interaction with the ExternalDataHandler framework"""

    def __init__(data_provider=None, data_source=None, ext_dset=None, ds_desc=None, update_desc=None, *args, **kwargs):
        """
        Initializes this instance of ExternalDataHandler

        Subclasses of this interface must implement this method
        @param data_provider An instance of the ExternalDataProvider Resource
        @param data_source An instance of the DataSource Resource
        @param ext_dset An instance of the ExternalDataset Resource
        @param ds_desc An instance of the *DatasetDescription IonObject - the concrete implementation of this object is specific to the concrete *DataHandler
        @param update_desc An instance of the UpdateDescription IonObject
        """
        pass

    def get_status(**kwargs):
        """
        Returns the status of the External Observatory.
        @return Should be a member of the OBSERVATORY_* attribute set defined in BaseExternalDataHandler
        """
        pass

        ## This is not in the correct place - there is no reason for the DataHandler to have any knowledge of the catalog for a given data provider/source

    #    def get_catalog(**kwargs):
    #        """
    #        Returns a catalog of assets from the External Observatory
    #        """
    #        pass

    def has_new_data(**kwargs):
        """
        This method should use the get_signature and compare methods to determine if there is new data at the source that has not yet been acquired by the system

        @return A boolean indicating if there is new data available from the External Observatory
        """
        pass

    def acquire_data(request=None, **kwargs):
        """
        Acquires data based on the information in the request parameter.  The exact form of the request object is left up to the concrete implementation of the specific DataHandler

        @return <as defined by the Scientific Data Library Interface>
        """
        pass

    def acquire_new_data(**kwargs):
        """
        Acquires all new data from the source.  This method does not facilitate "subset" acquisition, it retrieves all data that is not contained in the internal representation of the dataset

        @return <as defined by the Scientific Data Library Interface>
        """
        pass

    def get_attributes(scope=None):
        """
        Gets the attributes based on the indicated scope

        @param scope An indicator of scope used by the concrete implementation to determine if "global" or "variable" attributes are to be returned

        @return a dictionary containing the name/value pairs for all attributes in the given scope.
        """
        pass

    def get_signature(recalculate=False, data_sampling=None, **kwargs):
        """
        Generates a "signature" of the dataset referenced by this ExternalDataHandler.
        It's precise form is determined by the concrete *ExternalDataHandler class.

        The signature should describe the data set in enough detail that changes
        can be detected by comparison of signatures.

        @param recalculate If True, the method should force recalculation of the signature
        @param data_sampling Indicates the type of data sampling that should be used when generating the signature - a member of the BaseExternalDataHandler.DATA_SAMPLING* attribute set should be used.  If BaseExternalDataHandler.DATA_SAMPLING_SHOTGUN is provided, a kwarg ==> BaseExternalDataHandler.DATA_SAMPLING_SHOTGUN_COUNT: (int) should also be provided

        @return The signature for the dataset referenced by this ExternalDataHandler
        """

    def compare(external_data_handler=None):
        """
        Compares the signature of this instance of ExternalDataHandler with the passed instance.
        At a minimum, the compare method should return an indicator that can be used to determine if there
        is new data available from the ExternalDataProducer.


        """
        pass