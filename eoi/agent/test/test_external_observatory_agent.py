#!/usr/bin/env python

"""
@package eoi.agent.test
@file test_external_observatory_agent
@author Christopher Mueller
@brief 
"""
from pyon.net.endpoint import ProcessRPCClient

__author__ = 'Christopher Mueller'
__licence__ = 'Apache 2.0'

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from pyon.public import log, PRED
from mock import Mock
from nose.plugins.attrib import attr
from pyon.util.int_test import IonIntegrationTestCase
from interface.objects import ExternalDataSourceModel, ExternalDataset, ExternalDataProvider, DataSource, Institution, ContactInformation, DatasetDescription, UpdateDescription, Stream, AgentCommand
from interface.messages import external_observatory_agent_execute_in, external_observatory_agent_get_capabilities_in

@attr('INT', group='eoi-agt')
class TestIntExternalObservatoryAgent(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url(rel_url='res/deploy/r2eoi.yml')

        self.rr_cli = ResourceRegistryServiceClient()
        self.dams_cli = DataAcquisitionManagementServiceClient()

        self._setup_ncom()
        proc_name = self.ncom_ds_id+"_worker"
        config = {}
        config['process']={'name':proc_name,'type':'agent'}
        config['process']['eoa']={'dataset_id':self.ncom_ds_id}
        pid = self.container.spawn_process(name=proc_name, module='eoi.agent.external_observatory_agent', cls='ExternalObservatoryAgent', config=config)
        queue_id = "%s.%s" % (self.container.id, pid)

        log.debug("Spawned worker process ==> proc_name: %s\tproc_id: %s\tqueue_id: %s" % (proc_name, pid, queue_id))

        self._agent_cli = ProcessRPCClient(name=queue_id, process=self.container)

    def _setup_ncom(self):
        # TODO: some or all of this (or some variation) should move to DAMS

        # Create and register the necessary resources/objects

        # Create DataProvider
        dprov = ExternalDataProvider(institution=Institution(), contact=ContactInformation())
        #        dprov.institution.name = "OOI CGSN"
        dprov.contact.name = "Robert Weller"
        dprov.contact.email = "rweller@whoi.edu"

        # Create DataSource
        dsrc = DataSource(protocol_type="DAP", institution=Institution(), contact=ContactInformation())
        #        dsrc.connection_params["base_data_url"] = "http://ooi.whoi.edu/thredds/dodsC/"
        dsrc.connection_params["base_data_url"] = ""
        dsrc.contact.name="Rich Signell"
        dsrc.contact.email = "rsignell@usgs.gov"

        # Create ExternalDataset
        dset = ExternalDataset(name="test", dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())

        #        dset.dataset_description.parameters["dataset_path"] = "ooi/AS02CPSM_R_M.nc"
        dset.dataset_description.parameters["dataset_path"] = "test_data/ncom.nc"
        dset.dataset_description.parameters["temporal_dimension"] = "time"
        dset.dataset_description.parameters["zonal_dimension"] = "lon"
        dset.dataset_description.parameters["meridional_dimension"] = "lat"

        # Create ExternalDataSourceModel
        dsrc_model = ExternalDataSourceModel(name="dap_model")
        dsrc_model.model = "DAP"
        dsrc_model.data_handler_module = "eoi.agent.handler.dap_external_data_handler"
        dsrc_model.data_handler_class = "DapExternalDataHandler"

        ## Run everything through DAMS
        #TODO: Uncomment when CRUD methods in DAMS are implemented
        #        self.ncom_ds_id = self.dams_cli.create_external_dataset(external_dataset=dset)
        #        ext_dprov_id = self.dams_cli.create_external_data_provider(external_data_provider=dprov)
        #        ext_dsrc_id = self.dams_cli.create_data_source(data_source=dsrc)

        self.ncom_ds_id, _ = self.rr_cli.create(dset)
        ext_dprov_id, _ = self.rr_cli.create(dprov)
        ext_dsrc_id, _ = self.rr_cli.create(dsrc)
        #TODO: this needs to be added to DAMS
        ext_dsrc_model_id, _ = self.rr_cli.create(dsrc_model)

        ## Associate everything
        self.rr_cli.create_association(self.ncom_ds_id, PRED.hasSource, ext_dsrc_id)
        log.debug("Associated ExternalDataset %s with DataSource %s" % (self.ncom_ds_id, ext_dsrc_id))
        self.rr_cli.create_association(ext_dsrc_id, PRED.hasProvider, ext_dprov_id)
        log.debug("Associated DataSource %s with ExternalDataProvider %s" % (ext_dsrc_id, ext_dprov_id))
        self.rr_cli.create_association(ext_dsrc_id, PRED.hasModel, ext_dsrc_model_id)
        log.debug("Associated DataSource %s with ExternalDataSourceModel %s" % (ext_dsrc_id, ext_dsrc_model_id))
        data_prod_id = self.dams_cli.register_external_data_set(self.ncom_ds_id)
        log.debug("Registered ExternalDataset {%s}: DataProducer ID = %s" % (self.ncom_ds_id, data_prod_id))

########## Tests ##########

#    @unittest.skip("")
    def test_get_capabilities(self):
        res_id = self.ncom_ds_id

        # Get all the capabilities
        exe = external_observatory_agent_get_capabilities_in(resource_id=res_id, capability_types=None)
        log.debug("get_capabilities: eoa_get_caps_in=%s" % exe)
        caps = self._agent_cli.request(exe, op='get_capabilities')
        log.debug("all capabilities: %s" % caps)
        self.assertEqual(type(caps), list)

        exe = external_observatory_agent_get_capabilities_in(resource_id=res_id, capability_types=['RES_CMD'])
        log.debug("get_capabilities: eoa_get_caps_in=%s" % exe)
        caps = self._agent_cli.request(exe, op='get_capabilities')
        log.debug("resource commands: %s" % caps)
        self.assertEqual(type(caps), list)

        exe = external_observatory_agent_get_capabilities_in(resource_id=res_id, capability_types=['RES_PAR'])
        log.debug("get_capabilities: eoa_get_caps_in=%s" % exe)
        caps = self._agent_cli.request(exe, op='get_capabilities')
        log.debug("resource parameters: %s" % caps)
        self.assertEqual(type(caps), list)

        exe = external_observatory_agent_get_capabilities_in(resource_id=res_id, capability_types=['AGT_CMD'])
        log.debug("get_capabilities: eoa_get_caps_in=%s" % exe)
        caps = self._agent_cli.request(exe, op='get_capabilities')
        log.debug("agent commands: %s" % caps)
        self.assertEqual(type(caps), list)

        exe = external_observatory_agent_get_capabilities_in(resource_id=res_id, capability_types=['AGT_PAR'])
        log.debug("get_capabilities: eoa_get_caps_in=%s" % exe)
        caps = self._agent_cli.request(exe, op='get_capabilities')
        log.debug("agent parameters: %s" % caps)
        self.assertEqual(type(caps), list)

#    @unittest.skip("")
    def test_execute_get_attrs(self):
        res_id = self.ncom_ds_id

        cmd = AgentCommand(command_id="111", command="get_attributes")
        log.debug("Execute AgentCommand: %s" % cmd)
        exe = external_observatory_agent_execute_in(resource_id=res_id, command=cmd)
        log.debug("execute: eoa_execute_in=%s" % exe)
        ret = self._agent_cli.request(exe, op='execute')
        log.debug("Returned: %s" % ret)
        self.assertEquals(ret.status, 0)
        self.assertTrue(type(ret.result), dict)
