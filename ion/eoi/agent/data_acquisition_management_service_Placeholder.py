__author__ = 'timgiguere'

from ion.eoi.agent.handler.dap_external_data_handler \
    import DapExternalDataHandler
from pyon.core.bootstrap import IonObject
from pyon.public import RT

HFR = "hfr"
KOKAGG = "kokagg"
AST2 = "ast2"
SSTA = "ssta"
GHPM = "ghpm"
COMP1 = "comp1"
COMP2 = "comp2"


class DataAcquisitionManagementServicePlaceholder:

    def get_data_handlers(self, ds_id=''):

        external_data_provider = self.create_external_data_provider(ds_id)
        data_source = self.create_data_source(ds_id)
        external_data_set = self.create_external_data_set(ds_id)
        dap_ds_desc = self.create_dap_ds_desc(ds_id)

        dsh = DapExternalDataHandler(external_data_provider,
                                     data_source,
                                     external_data_set,
                                     dap_ds_desc)

        return dsh

    def create_external_data_provider(self, ds_id=''):
        dprov = {}
        if ds_id == HFR:
            dprov["institution_name"] = "HFRNET UCSD"
            dprov["institution_id"] = "342"
        elif ds_id == KOKAGG:
            dprov["institution_name"] = "University of Hawaii"
            dprov["institution_id"] = "128"
        elif ds_id == AST2:
            dprov["institution_name"] = "OOI CGSN"
            dprov["institution_id"] = "5"
            dprov["contact_name"] = "Robert Weller"
            dprov["contact_email"] = "rweller@whoi.edu"
        elif ds_id == SSTA:
            dprov["institution_name"] = "Remote Sensing Systems"
            dprov["institution_id"] = "848"
            dprov["contact_email"] = "support@gmss.com"

        return IonObject(RT.ExternalDataProvider, dprov)

    def read_external_data_provider(self, ds_id=''):
        # Return Value
        # ------------
        # ExternalDataProvider: {}
        #
        pass

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

    def create_data_source(self, ds_id=''):
        dsrc = {}
        if ds_id == HFR:
            dsrc["protocol_type"] = "DAP"
            dsrc["base_data_url"] = "http://hfrnet.ucsd.edu:8080/thredds/dodsC/"
        elif ds_id == KOKAGG:
            dsrc["protocol_type"] = "DAP"
            dsrc["base_data_url"] = "http://oos.soest.hawaii.edu/thredds/dodsC/"
        elif ds_id == AST2:
            dsrc["protocol_type"] = "DAP"
            dsrc["base_data_url"] = "http://ooi.whoi.edu/thredds/dodsC/"
            dsrc["contact_name"] = "Rich Signell"
            dsrc["contact_email"] = "rsignell@usgs.gov"
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

        return IonObject(RT.DataSource, dsrc)

    def read_data_source(self, ds_id=''):
        # Return Value
        # ------------
        # DataSource: {}
        #
        pass

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

    def create_external_data_set(self, ds_id=''):
        dset = {}
        if ds_id == KOKAGG:
            dset["contact_name"] = "Pierre Flament"
            dset["contact_email"] = "pflament@hawaii.edu"

        return IonObject(RT.ExternalDataset, dset)

    def read_external_data_set(self, ds_id=''):
        # Return Value
        # ------------
        # ExternalDataSet: {}
        #
        pass

    def delete_external_data_set(self, ds_id=''):
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def update_external_data_set(self, external_data_set={}):
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def create_dap_ds_desc(self, ds_id=''):
        dsdesc = {}
        if ds_id == HFR:
            dsdesc["dataset_path"] = "HFRNet/USEGC/6km/hourly/RTV"
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
            dsdesc["dataset_path"] = "/Users/timgiguere/Documents/Dev/sample_data/ast2_ghpm_spp_ctd.nc_1"
            dsdesc["temporal_dimension"] = "time"
            dsdesc["zonal_dimension"] = "lon"
            dsdesc["meridional_dimension"] = "lat"
        elif ds_id == COMP1:
            dsdesc["dataset_path"] = "/Users/timgiguere/Documents/Dev/sample_data/ast2_ghpm_spp_ctd.nc_1"
            dsdesc["temporal_dimension"] = "time"
            dsdesc["zonal_dimension"] = "lon"
            dsdesc["meridional_dimension"] = "lat"
        elif ds_id == COMP2:
            dsdesc["dataset_path"] = "/Users/timgiguere/Documents/Dev/sample_data/ast2_ghpm_spp_ctd.nc_2"
            dsdesc["temporal_dimension"] = "time"
            dsdesc["zonal_dimension"] = "lon"
            dsdesc["meridional_dimension"] = "lat"

        return IonObject("DapDatasetDescription", dsdesc)

    def read_dap_ds_desc(self, ds_id=''):
        # Return Value
        # ------------
        # DapDatasetDescription: {}
        #
        pass

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