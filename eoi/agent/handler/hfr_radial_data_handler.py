#!/usr/bin/env python

__author__ = 'tgiguere'

from pyon.public import log
from interface.objects import Variable, Attribute
from eoi.agent.handler.ascii_external_data_handler import *
import numpy


class HfrRadialDataHandler(AsciiExternalDataHandler):

    def __init__(self, data_provider=None, data_source=None, ext_dataset=None, *args, **kwargs):
        AsciiExternalDataHandler.__init__(self, data_provider, data_source, ext_dataset, *args, **kwargs)
        #TODO: Verify parameters as appropriate IonObjects

        import os
        self._variables = []
        self._global_attributes = []
        self._data_source = os.path.basename(data_source)
        self._load_attributes(data_source)
        self._load_values(data_source, '%')

    def _load_attributes(self, filename=''):
        import urllib
        column_names = []
        #looping through the whole file to get the attributes; not sure if this is such a good idea
        if filename.startswith('http'):
            f = urllib.urlopen(filename)
        else:
            f = open(filename, 'r')

        in_table_data = False
        correct_table_type = False
        variables_populated = False
        for line in f:
            if in_table_data:
                parsed_line = line.replace('%%', '')
                if len(column_names) == 0:
                    column_names = parsed_line.split()
                    column_names.reverse()
                elif not variables_populated:
                    units = parsed_line.split()
                    units.reverse()
                    index = 0
                    for var in self._variables:
                        var.units = units.pop()
                        attr = Attribute()
                        attr.name = 'units'
                        attr.value = var.units
                        var.attributes.append(attr)
                        attr = Attribute()
                        attr.name = 'long_name'
                        attr.value = column_names.pop()
                        if attr.value == 'U' or attr.value == 'V' or attr.value == 'X' or attr.value == 'Y':
                            attr.value += ' ' + column_names.pop()
                        var.attributes.append(attr)
                    variables_populated = True

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


            if not in_table_data:
                self._parse_attribute(line, correct_table_type, self._variables, self._global_attributes)
        f.close()

    def _parse_attribute(self, line='', correct_table_type=False, variables=None, attributes=None):
        #strip out leading %
        new_line = line.replace('%', '')

        parsed_line = new_line.partition(':')
        if parsed_line[0] == 'TableColumnTypes' and correct_table_type:
            cols = parsed_line[2].split(' ')
            for col in cols:
                if not col == '' and not col == '\n':
                    var = Variable()
                    var.attributes = []
                    var.column_name = col
                    #var.key = col
                    self._variables.append(var)
        elif not parsed_line[0].startswith('Table'):
            if not parsed_line[2] == '':
                att = Attribute()
                att.name = parsed_line[0]
                att.value = parsed_line[2].replace('\n', '')
                attributes.append(att)

    #def acquire_data_by_request(self, request=None, **kwargs):
    #    """
    #    Returns data based on a request containing the name of a variable (request.name) and a tuple of slice objects (slice_)
    #    @param request An object (nominally an IonObject of type PydapVarDataRequest) with "name" and "slice" attributes where: "name" == the name of the variable; and "slice" is a tuple ('var_name, (slice_1(), ..., slice_n()))
    #    """
    #    name = request.name
    #    slice_ = request.slice
    #    typecode = ''
    #    dims = []
    #
    #    if name in self._variables:
    #        var = self._variables[name]
    #        if var.shape:
    #            data = ArrayIterator(var, self._block_size)[slice_]
    #        else:
    #            data = numpy.array(var.getValue())
    #        typecode = var.dtype.char
    #        #dims = var.dimensions
    #        attrs = self.get_attributes()
    #
    #    return name, data, typecode, dims, attrs