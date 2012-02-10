__author__ = 'tgiguere'

from eoi.agent.handler.base_external_data_handler import BaseExternalDataHandler
from interface.objects import DatasetDescriptionDataSamplingEnum, CompareResult, CompareResultEnum
from eoi.agent.utils import ArrayIterator
import numpy
import hashlib

class AsciiExternalDataHandler(BaseExternalDataHandler):

    def __init__(self, data_provider=None, data_source=None, ext_dataset=None, *args, **kwargs):
        BaseExternalDataHandler.__init__(self, data_provider, data_source, ext_dataset, *args, **kwargs)

        self._number_of_records = 0
        self._data_array = None

    def _load_values(self, filename='', comments=''):
        self._data_array = numpy.genfromtxt(fname=filename, comments=comments)

        index = 0
        for var in self._variables:
            var.key = index
            index += 1

        self._number_of_records = self._data_array.shape[0]

    def get_attributes(self, scope=None):
        """
        Returns a dictionary containing the name/value pairs for all attributes in the given scope.
        @param scope The name of a variable in this dataset.  If no scope is provided, returns the global_attributes for the dataset
        """
        #Since there are no variable attributes in this file, just return the global ones.
        result = None
        if scope is None:
            result = self._global_attributes
        else:
            for var in self._variables:
                if var.column_name == scope:
                    result = var.attributes

        return result

    def get_variable_data(self, key=''):
        return self._data_array[:,key]

    def acquire_data(self, var_name=None, slice_=()):

        if not isinstance(slice_, tuple): slice_ = (slice_,)

        vars = self._variables

        if not var_name is None:
            for var in self._variables:
                if var.column_name == var_name:
                    vars = [var]
                    break

        for vn in vars:
            var = self._data_array[:,vn.key]

            ndims = len(var.shape)
            # Ensure the slice_ is the appropriate length
            if len(slice_) < ndims:
                slice_ += (slice(None),) * (ndims-len(slice_))

            arri = ArrayIterator(var, self._block_size)[slice_]
            for d in arri:
                if d.dtype.char is "S":
                    # Obviously, we can't get the range of values for a string data type!
                    rng = None
                elif isinstance(d, numpy.ma.masked_array):
                    # TODO: This is a temporary fix because numpy 'nanmin' and 'nanmax'
                    # are currently broken for masked_arrays:
                    # http://mail.scipy.org/pipermail/numpy-discussion/2011-July/057806.html
                    dc = d.compressed()
                    if dc.size == 0:
                        rng = None
                    else:
                        rng = (numpy.nanmin(dc), numpy.nanmax(dc))
                else:
                    rng = (numpy.nanmin(d), numpy.nanmax(d))
                yield vn, arri.curr_slice, rng, d

        return