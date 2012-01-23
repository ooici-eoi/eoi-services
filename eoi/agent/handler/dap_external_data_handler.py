#!/usr/bin/env python

__author__ = 'cmueller'

from interface.objects import DatasetDescriptionDataSamplingEnum
from eoi.agent.handler.base_external_data_handler import *
from eoi.agent.utils import ArrayIterator
from netCDF4 import Dataset
import cdms2
import hashlib
import numpy

class DapExternalDataHandler(BaseExternalDataHandler):

    _ds_url = None
    _ds = None
    _cdms_ds = None
    _tvar = None

    def __init__(self, data_provider=None, data_source=None, ext_dataset=None, *args, **kwargs):
        BaseExternalDataHandler.__init__(self, data_provider, data_source, ext_dataset, *args, **kwargs)

        if self._ext_dataset_res is not None:
            base_url=""
            if self._ext_data_source_res is not None and "base_data_url" in self._ext_data_source_res.connection_params:
                base_url = self._ext_data_source_res.connection_params["base_data_url"]
            self._ds_url = base_url + self._ext_dataset_res.dataset_description.parameters["dataset_path"]
        else:
            raise InstantiationError("Invalid DatasetHandler: ExternalDataset resource cannot be 'None'")

        self._ds = Dataset(self._ds_url)

    def get_attributes(self, scope=None):
        """
        Returns a dictionary containing the name/value pairs for all attributes in the given scope.
        @param scope The name of a variable in this dataset.  If no scope is provided, returns the global_attributes for the dataset
        """
        if (scope is None) or (scope not in self._ds.variables):
            var = self._ds
        else:
            var = self._ds.variables[scope]
        
        return dict((a, getattr(var, a)) for a in var.ncattrs())

    def acquire_data(self, var_name=None, slice_=()):
        if var_name is None:
            vars = self._ds.variables.keys()
        else:
            vars = [var_name]

        if not isinstance(slice_, tuple): slice_ = (slice_,)

        for vn in vars:
            var=self._ds.variables[vn]

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
                    dc=d.compressed()
                    if dc.size == 0:
                        rng = None
                    else:
                        rng = (numpy.nanmin(dc), numpy.nanmax(dc))
                else:
                    rng = (numpy.nanmin(d), numpy.nanmax(d))
                yield vn, arri.curr_slice, rng, d

        return

    def acquire_new_data(self):
        tvar = self.find_time_axis()

        #TODO: use the last state of the data to determine what's new and build an appropriate slice_
        slice_ = (slice(None),)

        return self.acquire_data(slice_=slice_)

    def acquire_data_by_request(self, request=None, **kwargs):
        """
        Returns data based on a request containing the name of a variable (request.name) and a tuple of slice objects (slice_)
        @param request An object (nominally an IonObject of type PydapVarDataRequest) with "name" and "slice" attributes where: "name" == the name of the variable; and "slice" is a tuple ('var_name, (slice_1(), ..., slice_n()))
        """
        name = request.name
        slice_ = request.slice

        if name in self._ds.variables:
            var = self._ds.variables[name]
            # Must turn off auto mask&scale - causes downstream issues if left on (default)
            var.set_auto_maskandscale(False)
            if var.shape:
                data = ArrayIterator(var, self._block_size)[slice_]
            else:
                data = numpy.array(var.getValue())
            typecode = var.dtype.char
            dims = var.dimensions
            attrs = self.get_attributes(scope=name)
        else:
#            print "====> HERE WE ARE"
            #TODO:  Not really sure what the heck this is doing...seems to be making up data??
            size = None
            for var in self._ds.variables:
                var = self._ds.variables[var]
                if name in var.dimensions:
                    size = var.shape[list(var.dimensions).index(name)]
                    break
            if size is None:
                raise DataAcquisitionError("The name '%s' does not appear to be available in the dataset, cannot acquire data" % name)

            data = numpy.arange(size)[slice_]
            typecode = data.dtype.char
            dims, attrs = (name,), {}

        if typecode == 'S1' or typecode == 'S':
            typecode = 'S'
            data = numpy.array([''.join(row) for row in numpy.asarray(data)])
            dims = dims[:-1]

        return name, data, typecode, dims, attrs

    def has_new_data(self, **kwargs):
        last_signature = self._ext_dataset_res.update_description.last_signature

        if last_signature is None or last_signature == "":
            return True

        # compare the last_signature to the current dataset _signature
        dcr = self.compare(last_signature)
        dcr_result = dcr.get_result()
        if dcr_result[0] in [dcr.EQUAL, dcr.MOD_GATT]:
            return False

        return True

    def get_signature(self, recalculate=False, **kwargs):
        """
        Calculate the _signature of the dataset
        """
#        if self._dataset_desc_obj.data_sampling is None or self._dataset_desc_obj.data_sampling == "":
#            data_sampling = BaseExternalDataHandler.DATA_SAMPLING_NONE
#        else:
#            data_sampling = self._dataset_desc_obj.data_sampling

        data_sampling = self._ext_dataset_res.dataset_description.data_sampling

        if recalculate:
            self._signature = None

        if self._signature is not None:
            return self._signature

        ret = {}
        # sha for time values
#        _tvar=self.find_time_axis()
#        if _tvar is not None:
#            sha_time=hashlib.sha1()
#            sha_time.update(str(_tvar[:]))
#            ret['times']=sha_time.hexdigest()
#        else:
#            ret['times']=None

        # sha for variables
        var_map = {}
        for vk in self._ds.variables:
        #        sha_vars.update(str(self._ds))
            var = self._ds.variables[vk]
            #            sha_vars.update(str(var)) # includes the "current size"
            var_sha = hashlib.sha1()
            var_atts = {}
            for ak in var.ncattrs():
                att = var.getncattr(ak)
                var_atts[ak] = hashlib.sha1(str(att)).hexdigest()
                var_sha.update(var_atts[ak])

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

            var_map[vk] = var_sha.hexdigest(), var_atts

        sha_vars = hashlib.sha1()
        for key in var_map:
            sha_vars.update(var_map[key][0])

        ret["vars"] = sha_vars.hexdigest(), var_map

        # sha for dimensions
        dim_map = {}
        for dk in self._ds.dimensions:
            dim = self._ds.dimensions[dk]
            dim_map[dk] = hashlib.sha1(str(dim)).hexdigest()

        sha_dim = hashlib.sha1()
        for key in dim_map:
            sha_dim.update(dim_map[key])

        ret["dims"] = sha_dim.hexdigest(), dim_map

        # sha for globals
        gbl_map = {}
        for gk in self._ds.ncattrs():
            gbl = self._ds.getncattr(gk)
            gbl_map[gk] = hashlib.sha1(str(gbl)).hexdigest()

        sha_gbl = hashlib.sha1()
        for key in gbl_map:
            sha_gbl.update(gbl_map[key])

        ret["gbl_atts"] = sha_gbl.hexdigest(), gbl_map

        sha_full = hashlib.sha1()
        for key in ret:
            if ret[key] is not None:
                sha_full.update(ret[key][0])

        self._signature = sha_full.hexdigest(), ret
        return self._signature

    def compare(self, data_signature):

        my_sig = self.get_signature(recalculate=True)

        dcr = DatasetComparisonResult()

        if my_sig[0] != data_signature[0]:
            #TODO: make info
            print "=!> Full signatures differ"
            if my_sig[1]["dims"][0] != data_signature[1]["dims"][0]:
                #TODO: make info
                print "==!> Dimensions differ"
                for dk in my_sig[1]["dims"][1]:
                    v1 = my_sig[1]["dims"][1][dk]
                    if dk in data_signature[1]["dims"][1]:
                        v2 = data_signature[1]["dims"][1][dk]
                    else:
                        #TODO: make info
                        print "===!> Dimension '%s' does not exist in 2nd dataset" % dk
                        dcr.add_dim(dk, True)
                        continue

                    if v1 != v2:
                        #TODO: make info
                        print "===!> Dimension '%s' differs" % dk
                        dcr.add_dim(dk)
                    else:
                        #TODO: make debug
#                        print "====> Dimension '%s' is equal" % dk
                        continue
            else:
                #TODO: make info
                print "===> Dimensions are equal"

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
                        dcr.add_gbl_attr(gk, True)
                        continue

                    if v1 != v2:
                        #TODO: make info
                        print "===!> Global Attribute '%s' differs" % gk
                        dcr.add_gbl_attr(gk)
                    else:
                        #TODO: make debug
#                        print "====> Global Attribute '%s' is equal" % gk
                        continue
                        
            else:
                #TODO: make info
                print "===> Global Attributes are equal"

            if my_sig[1]["vars"][0] != data_signature[1]["vars"][0]:
                #TODO: make info
                print "==!> Variable attributes differ"
                for vk in my_sig[1]["vars"][1]:
                    v1 = my_sig[1]["vars"][1][vk][0]
                    if vk in data_signature[1]["vars"][1]:
                        v2 = data_signature[1]["vars"][1][vk][0]
                    else:
                        #TODO: make info
                        print "===!> Variable '%s' does not exist in 2nd dataset" % vk
                        continue

                    if v1 != v2:
                        #TODO: make info
                        print "===!> Variable '%s' differ" % vk
                        for vak in my_sig[1]["vars"][1][vk][1]:
                            va1 = my_sig[1]["vars"][1][vk][1][vak]
                            if vak in data_signature[1]["vars"][1][vk][1]:
                                va2 = data_signature[1]["vars"][1][vk][1][vak]
                            else:
                                #TODO: make info
                                print "====!> Variable Attribute '%s' does not exist in 2nd dataset" % vak
                                dcr.add_var_attr(vak, True)
                                continue

                            if va1 != va2:
                                #TODO: make info
                                print "====!> Variable Attribute '%s' differs" % vak
                                dcr.add_var_attr(vak)
                            else:
                                #TODO: make debug
#                                print "======> Variable Attribute '%s' is equal" % vak
                                continue
            else:
                #TODO: make info
                print "===> Variable attributes are equal"

        else:
            #TODO: make debug
            print "==> Datasets are equal"

        return dcr

    def find_time_axis(self):
        if self._tvar is None:
            tdim = self._ext_dataset_res.dataset_description.parameters["temporal_dimension"]
            if tdim in self._ds.dimensions:
                if tdim in self._ds.variables:
                    self._tvar = self._ds.variables[tdim]
                else:
                    # find the first variable that has the tdim as the outer dimension
                    for vk in self._ds.variables:
                        var = self._ds.variables[vk]
                        if tdim in var.dimensions and var.dimensions.index(tdim) == 0:
                            self._tvar = var
                            break
            else:
                # try to figure out which is the time axis based on standard attributes
                if self._cdms_ds is None:
                    self._cdms_ds = cdms2.openDataset(self._ds_url)

                taxis = self._cdms_ds.getAxis('time')
                if taxis is not None:
                    self._tvar = self._ds.variables[taxis.id]
                else:
                    self._tvar = None

        return self._tvar

    def _pprint_signature(self):
        sig = self.get_signature()
        nl = "\n"
        str_lst = []
        str_lst.append(sig[0])
        str_lst.append(nl)
        str_lst.append("\tdims: ")
        str_lst.append(sig[1]["dims"][0])
        str_lst.append(nl)
        for dk in sig[1]["dims"][1]:
            str_lst.append("\t\t")
            str_lst.append(dk)
            str_lst.append(": ")
            str_lst.append(sig[1]["dims"][1][dk])
            str_lst.append(nl)

        str_lst.append("\tgbl_atts: ")
        str_lst.append(sig[1]["gbl_atts"][0])
        str_lst.append(nl)
        for gk in sig[1]["gbl_atts"][1]:
            str_lst.append("\t\t")
            str_lst.append(gk)
            str_lst.append(": ")
            str_lst.append(sig[1]["gbl_atts"][1][gk])
            str_lst.append(nl)

        str_lst.append("\tvars: ")
        str_lst.append(sig[1]["vars"][0])
        str_lst.append(nl)
        for vk in sig[1]["vars"][1]:
            str_lst.append("\t\t")
            str_lst.append(vk)
            str_lst.append(": ")
            str_lst.append(sig[1]["vars"][1][vk][0])
            str_lst.append(nl)
            for vak in sig[1]["vars"][1][vk][1]:
                str_lst.append("\t\t\t")
                str_lst.append(vak)
                str_lst.append(": ")
                str_lst.append(sig[1]["vars"][1][vk][1][vak])
                str_lst.append(nl)

        return "".join(str_lst)

    def __repr__(self):
#        return "%s\n***\ndataset keys: %s" % (BaseExternalObservatoryHandler.__repr__(self), self._ds.keys())
        return "%s\n***\ndataset:\n%s\ntime_var: %s\ndataset_signature(sha1): %s" % (BaseExternalDataHandler.__repr__(self), self._ds, str(self.find_time_axis()), self._pprint_signature())

#    def acquire_data_old(self, request=None, **kwargs):
#        """
#        For testing only at this point.  Called from examples/eoi/func_tst_update.py
#        """
#        td = timedelta(hours=-1)
#        edt = datetime.utcnow()
#        sdt = edt + td
#
#        if request is not None:
#            if "start_time" in request:
#                sdt = request.start_time
#            if "end_time" in request:
#                edt = request.end_time
#            if "lower_left_x" in request.bbox:
#                lower_left_x = request.bbox["lower_left_x"]
#            if "lower_left_y" in request.bbox:
#                lower_left_y = request.bbox["lower_left_y"]
#            if "upper_right_x" in request.bbox:
#                upper_right_x = request.bbox["upper_right_x"]
#            if "upper_right_y" in request.bbox:
#                upper_right_y = request.bbox["upper_right_y"]
#
#        tvar = self._ds.variables[self._dataset_desc_obj.temporal_dimension]
#        tindices = date2index([sdt, edt], tvar, 'standard', 'nearest')
#        if tindices[0] == tindices[1]:
#            # add one to the end index to ensure we get everything
#            tindices[1] += 1
#
#        print ">>> tindices [start end]: %s" % tindices
#
#        dims = self._ds.dimensions.items()
#
#        ret = {}
#        #        ret["times"] = _tvar[sti:eti]
#        for vk in self._ds.variables:
#            print "***"
#            print vk
#            var = self._ds.variables[vk]
#            t_idx = -1
#            lon_idx = -1
#            lat_idx = -1
#            if self._dataset_desc_obj.temporal_dimension in var.dimensions:
#                t_idx = var.dimensions.index(self._dataset_desc_obj.temporal_dimension)
#            if self._dataset_desc_obj.zonal_dimension in var.dimensions:
#                lon_idx = var.dimensions.index(self._dataset_desc_obj.zonal_dimension)
#            if self._dataset_desc_obj.meridional_dimension in var.dimensions:
#                lat_idx = var.dimensions.index(self._dataset_desc_obj.meridional_dimension)
#
#            lst = []
#            for i in range(len(var.dimensions)):
#                if i == t_idx:
#                    lst.append(slice(tindices[0], tindices[1]))
#                elif i == lon_idx:
#                    lst.append(slice(numpy.logical_and(self._dataset_desc_obj.zonal_dimension >= lower_left_x,
#                        self._dataset_desc_obj.zonal_dimension <= upper_right_x)))
#                elif i == lat_idx:
#                    lst.append(slice(numpy.logical_and(self._dataset_desc_obj.meridional_dimension >= lower_left_y,
#                        self._dataset_desc_obj.meridional_dimension <= upper_right_y)))
#                else:
#                    lst.append(slice(0, len(dims[i][1])))
#            print lst
#            print "==="
#            tpl = tuple(lst)
#            ret[vk] = tpl, var[tpl]
#            print ret[vk]
#            print "***"
#
#        #                if idx == 0:
#        #                    ret[vk] = var[sti:eti]
#        #                elif idx == 1:
#        #                    ret[vk] = var[:, sti:eti]
#        #                elif idx == 2:
#        #                    ret[vk] = var[:, :, sti:eti]
#        #                else:
#        #                    ret[vk] = "Temporal index > 2, WTF"
#        #            else:
#        #                ret[vk] = "Non-temporal Dimension ==> ignore for now"
#        #                continue
#
#        return ret

class DatasetComparisonResult():

    EQUAL = "EQUAL"
    NEW_DIM = "NEW_DIM"
    MOD_DIM = "MOD_DIM"
    NEW_GATT = "NEW_GATT"
    MOD_GATT = "MOD_GATT"
    NEW_VARATT = "NEW_VARATT"
    MOD_VARATT = "MOD_VARATT"
    MOD_DATA = "MOD_DATA"

    def __init__(self):
        self.new_dims = []
        self.mod_dims = []
        self.new_gatts = []
        self.mod_gatts = []
        self.new_varatts = []
        self.mod_varatts = []
        self.mod_data = []


    def add_dim(self, dim_name, is_new=False):
        if is_new:
            self.new_dims.append(dim_name)
        else:
            self.mod_dims.append(dim_name)

    def add_gbl_attr(self, gbl_attr_name, is_new=False):
        if is_new:
            self.new_gatts.append(gbl_attr_name)
        else:
            self.mod_gatts.append(gbl_attr_name)

    def add_var_attr(self, var_attr_name, is_new=False):
        if is_new:
            self.new_varatts.append(var_attr_name)
        else:
            self.mod_varatts.append(var_attr_name)

    def get_result(self):
        if self.new_dims:
            return self.NEW_DIM, self.new_dims
        elif self.mod_dims:
            return self.MOD_DIM, self.mod_dims
        elif self.mod_data:
            return self.MOD_DATA, self.mod_data
        elif self.new_varatts:
            return self.NEW_VARATT, self.new_varatts
        elif self.mod_varatts:
            return self.MOD_VARATT, self.mod_varatts
        elif self.new_gatts:
            return self.NEW_GATT, self.new_gatts
        elif self.mod_gatts:
            return self.MOD_GATT, self.mod_gatts
        else:
            return self.EQUAL, []


#    def walk_dataset(self, top):
#        values = top.group.values()
#        yield values
#        for value in values:
#            for children in walk_dataset(value):
#                yield children
#
#    def print_dataset_tree(self):
#        if self._ds is None:
#            return "Dataset is None"
#
#        print self._ds.path, self._ds
#        for children in walk_dataset(self._ds):
#            for child in children:
#                print child.path, child

#    def get_dataset_size(self):
#        return self._ds
#        return reduce(lambda x, y: x * self.calculate_data_size(y), self._ds.keys())

#    def calculate_data_size(self, variable=None):
#        if variable is None and self._ds is not None:
#            variable = self._ds[self._ds.keys()[0]]
#
#        if type(variable) == pydap.model.GridType:
#            return reduce(lambda x, y: x * y, variable.shape) * variable.type.size
#        else:
#            log.warn("variable is of unhandled type: %s" % type(variable))
