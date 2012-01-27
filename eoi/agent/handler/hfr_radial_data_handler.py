#!/usr/bin/env python

__author__ = 'tgiguere'

from pyon.public import log
from interface.objects import DatasetDescriptionDataSamplingEnum
from eoi.agent.handler.base_external_data_handler import *
from eoi.agent.utils import ArrayIterator
import numpy

class HfrRadialDataHandler(BaseExternalDataHandler):

    _data_array = None
    _number_of_records = 0
    _variables = []
    _attributes = {}

    def __init__(self, data_provider=None, data_source=None, ext_dataset=None, *args, **kwargs):
        BaseExternalDataHandler.__init__(self, data_provider, data_source, ext_dataset, *args, **kwargs)

        self._variables[:] = []
        self._load_attributes(data_source)
        self._load_values(data_source)

    def _load_attributes(self, filename=''):
        #looping through the whole file to get the attributes; not sure if this is such a good idea
        with open(filename, 'r') as f:
            in_table_data = False
            correct_table_type = False
            for line in f:
                if line.startswith('%TableType:'):
                    parsed_line = line.partition(': ')
                    if parsed_line[2].startswith('LLUV'):
                        correct_table_type = True
                    else:
                        correct_table_type = False
                if line.startswith('%TableStart:'):
                    in_table_data = True
                if line.startswith('%TableEnd:') and in_table_data:
                    in_table_data = False
                    correct_table_type = False


                if not (in_table_data):
                    self._parse_attribute(line, correct_table_type)
        f.close()

    def _parse_attribute(self, line='', correct_table_type=False):
        #strip out leading %
        new_line = line.replace('%', '')

        parsed_line = new_line.partition(':')
        if parsed_line[0] == 'TableColumnTypes' and correct_table_type:
            cols = parsed_line[2].split(' ')
            for col in cols:
                if not col == '' and not col == '\n':
                    self._variables.append(col)
        elif not parsed_line[0].startswith('Table'):
            if not parsed_line[2] == '':
                self._attributes[parsed_line[0]] = parsed_line[2].replace('\n', '')

    def _load_values(self, filename=''):
        a = numpy.loadtxt(fname=filename, comments='%')

        self._data_array = {}
        index = 0
        for column in self._variables:
            self._data_array[column] = a[:,index]
            index += 1

        self._number_of_records = a.shape[0]

    def acquire_data(self, var_name=None, slice_=()):
        if var_name in self._variables:
            vars = [var_name]
        else:
            vars = self._variables

        if not isinstance(slice_, tuple): slice_ = (slice_,)

        for vn in vars:
            var = self._data_array[vn]

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

    def get_attributes(self, scope=None):
        """
        Returns a dictionary containing the name/value pairs for all attributes in the given scope.
        @param scope The name of a variable in this dataset.  If no scope is provided, returns the global_attributes for the dataset
        """
        #Since there are no variable attributes in this file, just return the global ones.
        return self._attributes

    def get_attribute(self, attr_name=''):
        if attr_name in self._attributes:
            return self._attributes[attr_name]
        else:
            return ''

    def get_variables(self):
        return self._variables
