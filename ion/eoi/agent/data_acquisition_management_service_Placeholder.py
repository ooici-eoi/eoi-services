__author__ = 'timgiguere'
__author__ = 'cmueller'

from ion.eoi.agent.handler.dap_external_data_handler \
    import DapExternalDataHandler
from pyon.core.bootstrap import IonObject
from pyon.public import RT
import os

HFR = "hfr"
HFR_LOCAL = "hfr_local"
KOKAGG = "kokagg"
AST2 = "ast2"
SSTA = "ssta"
GHPM = "ghpm"
COMP1 = "comp1"
COMP2 = "comp2"
COADS = "coads"
NCOM = "ncom"

class DataAcquisitionManagementServicePlaceholder:

    def get_data_handler(self, ds_id=''):
        external_data_provider = self.read_external_data_provider(ds_id)
        data_source = self.read_data_source(ds_id)
        external_data_set = self.read_external_dataset(ds_id)
        dap_ds_desc = self.read_dap_ds_desc(ds_id)

        protocol_type = data_source.protocol_type
        if protocol_type == "DAP":
            dsh = DapExternalDataHandler(external_data_provider,
                                         data_source,
                                         external_data_set,
                                         dap_ds_desc)
        else:
            raise Exception("Unknown Protocol Type: %s" % protocol_type)

        return dsh

    def create_external_data_provider(self, external_data_provider=None):
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_external_data_provider(self, ds_id=''):
        dprov = {}
        if ds_id == HFR or ds_id == HFR_LOCAL:
            inst = IonObject("Institution", name="HFRNET UCSD")
            dprov["institution"] = inst
        elif ds_id == KOKAGG:
            inst = IonObject("Institution", name="University of Hawaii")
            dprov["institution"] = inst
        elif ds_id == AST2:
            inst = IonObject("Institution", name="OOI CGSN")
            contact = IonObject(RT.ContactInformation, name="Robert Weller")
            contact.email = "rweller@whoi.edu"
            dprov["institution"] = inst
            dprov["contact"] = contact
        elif ds_id == SSTA:
            inst = IonObject("Institution", name="Remote Sensing Systems")
            contact = IonObject(RT.ContactInformation)
            contact.email = "support@gmss.com"
            dprov["institution"] = inst
            dprov["contact"] = contact
        if dprov:
            return IonObject(RT.ExternalDataProvider, dprov)
        else:
            return None

    def delete_external_data_provider(self, ds_id=''):
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def update_external_data_provider(self, external_data_provider={}):
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def create_data_source(self, data_source=None):
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_data_source(self, ds_id=''):
        dsrc = {}
        if ds_id == HFR:
            dsrc["protocol_type"] = "DAP"
            dsrc["base_data_url"] = "http://hfrnet.ucsd.edu:8080/thredds/dodsC/"
        if ds_id == HFR_LOCAL:
            dsrc["protocol_type"] = "DAP"
            dsrc["base_data_url"] = ""
        elif ds_id == KOKAGG:
            dsrc["protocol_type"] = "DAP"
            dsrc["base_data_url"] = "http://oos.soest.hawaii.edu/thredds/dodsC/"
        elif ds_id == AST2:
            dsrc["protocol_type"] = "DAP"
            dsrc["base_data_url"] = "http://ooi.whoi.edu/thredds/dodsC/"
            contact = IonObject(RT.ContactInformation, name="Rich Signell")
            contact.email = "rsignell@usgs.gov"
            dsrc["contact"] = contact
        elif ds_id == SSTA:
            dsrc["protocol_type"] = "DAP"
            dsrc["base_data_url"] = "http://thredds1.pfeg.noaa.gov/thredds/dodsC/"
        elif ds_id == GHPM:
            dsrc["protocol_type"] = "DAP"
            dsrc["base_data_url"] = ""
        elif ds_id == COMP1:
            dsrc["protocol_type"] = "DAP"
            dsrc["base_data_url"] = ""
        elif ds_id == COMP2:
            dsrc["protocol_type"] = "DAP"
            dsrc["base_data_url"] = ""
        elif ds_id == COADS:
            dsrc["protocol_type"] = "DAP"
            dsrc["base_data_url"] = ""
        elif ds_id == NCOM:
            dsrc["protocol_type"] = "DAP"
            dsrc["base_data_url"] = ""

        if dsrc:
            return IonObject(RT.DataSource, dsrc)
        else:
            return None

    def delete_data_source(self, ds_id=''):
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def update_data_source(self, data_source={}):
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def create_external_dataset(self, external_data_set=None):
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_external_dataset(self, ds_id=''):
        dset = {}
        if ds_id == KOKAGG:
            contact = IonObject(RT.ContactInformation, name="Pierre Flament")
            contact.email = "pflament@hawaii.edu"
            dset["contact"] = contact
        if dset:
            return IonObject(RT.ExternalDataset, dset)
        else:
            return None
        pass

    def delete_external_dataset(self, ds_id=''):
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def update_external_dataset(self, external_data_set={}):
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def create_dap_ds_desc(self, ds_id=''):
        # Return Value
        # ------------
        # {success: true}
        #
        pass


    def read_dap_ds_desc(self, ds_id=''):
        CWD = os.getcwd()
        dsdesc = {}
        if ds_id == HFR:
            dsdesc["dataset_path"] = "HFRNet/USEGC/6km/hourly/RTV"
            dsdesc["temporal_dimension"] = "time"
            dsdesc["zonal_dimension"] = "lon"
            dsdesc["meridional_dimension"] = "lat"
        if ds_id == HFR_LOCAL:
            dsdesc["dataset_path"] = CWD + "/test_data/hfr.nc"
            dsdesc["temporal_dimension"] = "time"
            dsdesc["zonal_dimension"] = "lon"
            dsdesc["meridional_dimension"] = "lat"
        elif ds_id == KOKAGG:
            dsdesc["dataset_path"] = "hioos/hfr/kokagg"
            dsdesc["temporal_dimension"] = "time"
            dsdesc["zonal_dimension"] = "lon"
            dsdesc["meridional_dimension"] = "lat"
        elif ds_id == AST2:
            dsdesc["dataset_path"] = "ooi/AS02CPSM_R_M.nc"
            dsdesc["temporal_dimension"] = "time"
            dsdesc["zonal_dimension"] = "lon"
            dsdesc["meridional_dimension"] = "lat"
        elif ds_id == SSTA:
            dsdesc["dataset_path"] = "satellite/GR/ssta/1day"
            dsdesc["temporal_dimension"] = "time"
            dsdesc["zonal_dimension"] = "lon"
            dsdesc["meridional_dimension"] = "lat"
        elif ds_id == GHPM:
            dsdesc["dataset_path"] = CWD + "/test_data/ast2_ghpm_spp_ctd_1.nc"
            dsdesc["temporal_dimension"] = "time"
            dsdesc["zonal_dimension"] = "lon"
            dsdesc["meridional_dimension"] = "lat"
        elif ds_id == COMP1:
            dsdesc["dataset_path"] = CWD + "/test_data/ast2_ghpm_spp_ctd_1.nc"
            dsdesc["temporal_dimension"] = "time"
            dsdesc["zonal_dimension"] = "lon"
            dsdesc["meridional_dimension"] = "lat"
        elif ds_id == COMP2:
            dsdesc["dataset_path"] = CWD + "/test_data/ast2_ghpm_spp_ctd_2.nc"
            dsdesc["temporal_dimension"] = "time"
            dsdesc["zonal_dimension"] = "lon"
            dsdesc["meridional_dimension"] = "lat"
        elif ds_id == COADS:
            dsdesc["dataset_path"] = CWD + "/test_data/coads.nc"
            dsdesc["temporal_dimension"] = "time"
            dsdesc["zonal_dimension"] = "lon"
            dsdesc["meridional_dimension"] = "lat"
        elif ds_id == NCOM:
            dsdesc["dataset_path"] = CWD + "/test_data/ncom.nc"
            dsdesc["temporal_dimension"] = "time"
            dsdesc["zonal_dimension"] = "lon"
            dsdesc["meridional_dimension"] = "lat"

        if dsdesc:
            return IonObject("DapDatasetDescription", dsdesc)
        else:
            return None

    def delete_dap_ds_desc(self, ds_id=''):
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def update_dap_ds_desc(self, dap_ds_desc={}):
        # Return Value
        # ------------
        # {success: true}
        #
        pass