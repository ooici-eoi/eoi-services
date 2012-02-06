#!/usr/bin/env python

__author__ = 'tgiguere'

from pyon.public import log
from interface.objects import DatasetDescriptionDataSamplingEnum, CompareResult, CompareResultEnum, Variable
from eoi.agent.handler.base_external_data_handler import *
from eoi.agent.utils import ArrayIterator
import numpy
import hashlib


class HfrRadialDataHandler(BaseExternalDataHandler):

    _data_source = ''
    _data_array = None
    _number_of_records = 0
    _variables = []
    _attributes = {}

    def __init__(self, data_provider=None, data_source=None, ext_dataset=None, *args, **kwargs):
        BaseExternalDataHandler.__init__(self, data_provider, data_source, ext_dataset, *args, **kwargs)
        #TODO: Verify parameters as appropriate IonObjects

        import os
        self._variables[:] = []
        self._attributes = {}
        self._data_source = os.path.basename(data_source)
        self._load_attributes(data_source)
        self._load_values(data_source)

    def _load_attributes(self, filename=''):
        column_names = []
        #looping through the whole file to get the attributes; not sure if this is such a good idea
        with open(filename, 'r') as f:
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
                            var.long_name = column_names.pop()
                            if var.long_name == 'U' or var.long_name == 'V' or var.long_name == 'X' or var.long_name == 'Y':
                                var.long_name += ' ' + column_names.pop()
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
                    self._parse_attribute(line, correct_table_type, self._variables, self._attributes)
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
                    var.column_name = col
                    self._variables.append(var)
        elif not parsed_line[0].startswith('Table'):
            if not parsed_line[2] == '':
                attributes[parsed_line[0]] = parsed_line[2].replace('\n', '')

    def _load_values(self, filename=''):
        a = numpy.loadtxt(fname=filename, comments='%')

        self._data_array = {}
        index = 0
        for var in self._variables:
            self._data_array[var.column_name] = a[:,index]
            index += 1

        self._number_of_records = a.shape[0]

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

    def acquire_data(self, var_name=None, slice_=()):

        if not isinstance(slice_, tuple): slice_ = (slice_,)

        vars = self._variables

        if not var_name is None:
            for var in self._variables:
                if var.column_name == var_name:
                    vars = [var]
                    break

        for vn in vars:
            var = self._data_array[vn.column_name]

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

    def get_signature(self, recalculate=False, **kwargs):
        """
        Calculate the _signature of the dataset
        """

        data_sampling = self._ext_dataset_res.dataset_description.data_sampling

        if recalculate:
            self._signature = None

        if self._signature is not None:
            return self._signature

        ret = {}

        # sha for variables
        var_map = {}
        for vk in self._variables:
        #        sha_vars.update(str(self._ds))
            var = self._data_array[vk.column_name]
            #            sha_vars.update(str(var)) # includes the "current size"

            var_sha = hashlib.sha1()
            var_atts = {}
            var_atts['units'] = hashlib.sha1(str(vk.units)).hexdigest()
            var_sha.update(var_atts['units'])

            if data_sampling is DatasetDescriptionDataSamplingEnum.FIRST_LAST:
                slice_first = []
                slice_last = []
                for s in var.shape:
                    # get the first...where outer dims == 0 and the innermost == 1
                    slice_first.append(slice(0, 1))
                    # get the last...where outer dims == max for dim
                    slice_last.append(slice(s-1,s))

                dat_f = var[slice_first]
                dat_l = var[slice_last]

                # add the string data arrays into the sha
                var_sha.update(str(dat_f))
                var_sha.update(str(dat_l))

            elif data_sampling is DatasetDescriptionDataSamplingEnum.FULL:
                var_sha.update(str(var[:]))
                pass
            elif data_sampling is DatasetDescriptionDataSamplingEnum.SHOTGUN:
                shotgun_count = kwargs[DatasetDescriptionDataSamplingEnum.SHOTGUN_COUNT] or 10
                pass
            else:
                pass

            var_map[vk.column_name] = var_sha.hexdigest(), var_atts

        sha_vars = hashlib.sha1()
        for key in var_map:
            sha_vars.update(var_map[key][0])

        ret["vars"] = sha_vars.hexdigest(), var_map

        # sha for globals
        gbl_map = {}
        for gk in self._attributes:
            gbl = self._attributes[gk]
            gbl_map[gk] = hashlib.sha1(str(gbl)).hexdigest()

        sha_gbl = hashlib.sha1()
        for key in gbl_map:
            sha_gbl.update(gbl_map[key])

        ret["gbl_atts"] = sha_gbl.hexdigest(), gbl_map

        # sha for uuid
        uuid_map = {}
        uuid_map['UUID'] = hashlib.sha1(self._attributes['UUID']).hexdigest()

        sha_uuid = hashlib.sha1()
        for key in uuid_map:
            sha_uuid.update(uuid_map[key])

        ret[self._data_source] = sha_uuid.hexdigest(), uuid_map

        sha_full = hashlib.sha1()
        for key in ret:
            if ret[key] is not None:
                sha_full.update(ret[key][0])

        self._signature = sha_full.hexdigest(), ret
        return self._signature

    def compare(self, data_signature):

        my_sig = self.get_signature(recalculate=True)
        result = []

        if my_sig[0] != data_signature[0]:
            #TODO: make info
            print "=!> Full signatures differ"

            if my_sig[1]["gbl_atts"][0] != data_signature[1]["gbl_atts"][0]:
                #TODO: make info
                print "==!> Global Attributes differ"
                for gk in my_sig[1]["gbl_atts"][1]:
                    v1 = my_sig[1]["gbl_atts"][1][gk]

                    if gk in data_signature[1]["gbl_atts"][1]:
                        v2 = data_signature[1]["gbl_atts"][1][gk]
                    else:
                        #TODO: make info
                        print "===!> Global Attribute '%s' does not exist in 2nd dataset" % gk
                        res = CompareResult()
                        res.field_name = gk
                        res.difference = CompareResultEnum.NEW_GATT
                        result.append(res)
                        #dcr.add_gbl_attr(gk, True)
                        continue

                    if v1 != v2:
                        #TODO: make info
                        print "===!> Global Attribute '%s' differs" % gk
                        res = CompareResult()
                        res.field_name = gk
                        res.difference = CompareResultEnum.MOD_GATT
                        result.append(res)
                        #dcr.add_gbl_attr(gk)
                    else:
                    #TODO: make debug
                    #                        print "====> Global Attribute '%s' is equal" % gk
                        continue

                for gk in data_signature[1]["gbl_atts"][1]:
                    v1 = data_signature[1]["gbl_atts"][1][gk]

                    if gk in my_sig[1]["gbl_atts"][1]:
                        v2 = my_sig[1]["gbl_atts"][1][gk]
                    else:
                        #TODO: make info
                        print "===!> Global Attribute '%s' does not exist in 1st dataset" % gk
                        res = CompareResult()
                        res.field_name = gk
                        res.difference = CompareResultEnum.NEW_GATT
                        result.append(res)
                        #dcr.add_gbl_attr(gk, True)
                        continue

                    if v1 != v2:
                        #TODO: make info
                        print "===!> Global Attribute '%s' differs" % gk
                        res = CompareResult()
                        res.field_name = gk
                        res.difference = CompareResultEnum.MOD_GATT
                        result.append(res)
                        #dcr.add_gbl_attr(gk)
                    else:
                    #TODO: make debug
                    #                        print "====> Global Attribute '%s' is equal" % gk
                        continue

            else:
                #TODO: make info
                print "===> Global Attributes are equal"

            if my_sig[1]["vars"][0] != data_signature[1]["vars"][0]:
                #TODO: make info
                #print "==!> Variable attributes differ"
                for vk in my_sig[1]["vars"][1]:
                    v1 = my_sig[1]["vars"][1][vk][0]
                    if vk in data_signature[1]["vars"][1]:
                        v2 = data_signature[1]["vars"][1][vk][0]
                    else:
                        #TODO: make info
                        print "===!> Variable '%s' does not exist in 2nd dataset" % vk
                        res = CompareResult()
                        res.field_name = vk
                        res.difference = CompareResultEnum.NEW_GATT
                        result.append(res)
                        continue

                    if v1 != v2:
                        #TODO: make info
                        print "===!> Variable '%s' differ" % vk
                        res = CompareResult()
                        res.field_name = vk
                        res.difference = CompareResultEnum.MOD_VAR
                        result.append(res)
                        for vak in my_sig[1]["vars"][1][vk][1]:
                            va1 = my_sig[1]["vars"][1][vk][1][vak]
                            if vak in data_signature[1]["vars"][1][vk][1]:
                                va2 = data_signature[1]["vars"][1][vk][1][vak]
                            else:
                                #TODO: make info
                                print "====!> Variable Attribute '%s' does not exist in 2nd dataset" % vak
                                res = CompareResult()
                                res.field_name = vak
                                res.difference = CompareResultEnum.NEW_VARATT
                                result.append(res)
                                #dcr.add_var_attr(vak, True)
                                continue

                            if va1 != va2:
                                #TODO: make info
                                print "====!> Variable Attribute '%s' differs" % vak
                                res = CompareResult()
                                res.field_name = vak
                                res.difference = CompareResultEnum.MOD_VARATT
                                result.append(res)
                                #dcr.add_var_attr(vak)
                            else:
                            #TODO: make debug
                            #                                print "======> Variable Attribute '%s' is equal" % vak
                                continue
            else:
                #TODO: make info
                print "===> Variable attributes are equal"

        else:
            #TODO: make debug
            res = CompareResult()
            res.field_name = ""
            res.difference = CompareResultEnum.EQUAL
            result.append(res)

        return result