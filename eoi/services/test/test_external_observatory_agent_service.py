#!/usr/bin/env python

"""
@package 
@file TestExternalObservatoryAgentService
@author Christopher Mueller
@brief 
"""

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.eoi.iexternal_observatory_agent_service import ExternalObservatoryAgentServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from pyon.util.containers import DotDict
from pyon.public import log, PRED
from pyon.core.exception import IonException
from pyon.util.unit_test import PyonTestCase
from mock import Mock
from nose.plugins.attrib import attr
import unittest
from pyon.util.int_test import IonIntegrationTestCase
from eoi.services.external_observatory_agent_service import ExternalObservatoryAgentService
from interface.objects import ExternalDataSourceModel, ExternalDataset, ExternalDataProvider, DataSource, Institution, ContactInformation, DatasetDescription, UpdateDescription, Stream, AgentCommand

__author__ = 'Christopher Mueller'
__licence__ = 'Apache 2.0'

@attr('UNIT',group='eoi-svcs')
class TestExternalObservatoryAgentService(PyonTestCase):
    
    def setUp(self):
        mock_clients = self._create_service_mock('external_observatory_agent')
        self.ext_obs_service = ExternalObservatoryAgentService()
        self.ext_obs_service.clients = mock_clients
        self.ext_obs_service.clients.pubsub_management = DotDict()
        self.ext_obs_service.clients.pubsub_management['XP'] = 'science.data'
        self.ext_obs_service.clients.pubsub_management['create_stream'] = Mock()
        self.ext_obs_service.clients.pubsub_management['create_subscription'] = Mock()
        self.ext_obs_service.clients.pubsub_management['register_producer'] = Mock()
        self.ext_obs_service.clients.pubsub_management['activate_subscription'] = Mock()
        self.ext_obs_service.clients.pubsub_management['read_subscription'] = Mock()
        self.ext_obs_service.container = DotDict()
        self.ext_obs_service.container['spawn_process'] = Mock()
        self.ext_obs_service.container['id'] = 'mock_container_id'
        self.ext_obs_service.container['proc_manager'] = DotDict()
        self.ext_obs_service.container.proc_manager['terminate_process'] = Mock()

        # CRUD Shortcuts
#        self.mock_rr_create = self.ext_obs_service.clients.resource_registry.create
#        self.mock_rr_read = self.ext_obs_service.clients.resource_registry.read
#        self.mock_rr_update = self.ext_obs_service.clients.resource_registry.update
#        self.mock_rr_delete = self.ext_obs_service.clients.resource_registry.delete
#        self.mock_rr_find = self.ext_obs_service.clients.resource_registry.find_objects
#        self.mock_rr_assoc = self.ext_obs_service.clients.resource_registry.find_associations
#        self.mock_rr_create_assoc = self.ext_obs_service.clients.resource_registry.create_association
#        self.mock_rr_del_assoc = self.ext_obs_service.clients.resource_registry.delete_association

        self.mock_pd_create = self.ext_obs_service.clients.process_dispatcher_service.create_process_definition
        self.mock_pd_read = self.ext_obs_service.clients.process_dispatcher_service.read_process_definition
        self.mock_pd_update = self.ext_obs_service.clients.process_dispatcher_service.update_process_definition
        self.mock_pd_delete = self.ext_obs_service.clients.process_dispatcher_service.delete_process_definition
        self.mock_pd_schedule = self.ext_obs_service.clients.process_dispatcher_service.schedule_process
        self.mock_pd_cancel = self.ext_obs_service.clients.process_dispatcher_service.cancel_process

        self.mock_ps_create_stream = self.ext_obs_service.clients.pubsub_management.create_stream
        self.mock_ps_create_sub = self.ext_obs_service.clients.pubsub_management.create_subscription
        self.mock_ps_register = self.ext_obs_service.clients.pubsub_management.register_producer
        self.mock_ps_activate = self.ext_obs_service.clients.pubsub_management.activate_subscription
        self.mock_ps_read_sub = self.ext_obs_service.clients.pubsub_management.read_subscription

        self.mock_cc_spawn = self.ext_obs_service.container.spawn_process
        self.mock_cc_terminate = self.ext_obs_service.container.proc_manager.terminate_process

    @unittest.skip("Not Working")
    def test_spawn_worker(self):

        #mocks
        self.mock_cc_spawn.return_value = 'mock_pid'
        self.mock_ps_read_sub.return_value = DotDict({'exchange_name':'mock_exchange'})
#        self.mock_rr_create.return_value = ('mock_eoas_id','junk')

        #execution
        res = self.ext_obs_service.spawn_worker('resource_id')
        log.debug(res)
        print res

        #assertions
#        self.mock_ps_read_sub.assert_called_once_with(resource_id='resource_id')
        self.assertTrue(False)
        self.assertTrue(self.mock_cc_spawn.called)
        self.assertEquals(res, 'mock_pid')



@attr('INT', group='eoi-svcs')
class TestIntExternalObservatoryAgentService(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
#        self.container.start_rel_from_url(rel_url='res/deploy/r2deploy.yml')
        self.container.start_rel_from_url(rel_url='res/deploy/r2eoi.yml')

        self.eoas_cli = ExternalObservatoryAgentServiceClient()
        self.rr_cli = ResourceRegistryServiceClient()
        self.pubsub_cli = PubsubManagementServiceClient()
        self.dams_cli = DataAcquisitionManagementServiceClient()

        self._setup_ncom()
        self._setup_hfr()

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

    def _setup_hfr(self):
        # TODO: some or all of this (or some variation) should move to DAMS

        # Create and register the necessary resources/objects

        # Create DataProvider
        dprov = ExternalDataProvider(institution=Institution(), contact=ContactInformation())
#        dprov.institution.name = "HFR UCSD"

        # Create DataSource
        dsrc = DataSource(protocol_type="DAP", institution=Institution(), contact=ContactInformation())
        dsrc.connection_params["base_data_url"] = "http://hfrnet.ucsd.edu:8080/thredds/dodsC/"

        # Create ExternalDataset
        dset = ExternalDataset(name="UCSD HFR", dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        dset.dataset_description.parameters["dataset_path"] = "HFRNet/USEGC/6km/hourly/RTV"
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

        self.hfr_ds_id, _ = self.rr_cli.create(dset)
        ext_dprov_id, _ = self.rr_cli.create(dprov)
        ext_dsrc_id, _ = self.rr_cli.create(dsrc)
        #TODO: this needs to be added to DAMS
        ext_dsrc_model_id, _ = self.rr_cli.create(dsrc_model)

        self.rr_cli.create_association(self.hfr_ds_id, PRED.hasSource, ext_dsrc_id)
        log.debug("Associated ExternalDataset %s with DataSource %s" % (self.hfr_ds_id, ext_dsrc_id))
        self.rr_cli.create_association(ext_dsrc_id, PRED.hasProvider, ext_dprov_id)
        log.debug("Associated DataSource %s with ExternalDataProvider %s" % (ext_dsrc_id, ext_dprov_id))
        self.rr_cli.create_association(ext_dsrc_id, PRED.hasModel, ext_dsrc_model_id)
        log.debug("Associated DataSource %s with ExternalDataSourceModel %s" % (ext_dsrc_id, ext_dsrc_model_id))
        data_prod_id = self.dams_cli.register_external_data_set(self.hfr_ds_id)
        log.debug("Registered ExternalDataset {%s}: DataProducer ID = %s" % (self.hfr_ds_id, data_prod_id))



########## Tests ##########

#    @unittest.skip("")
    def test_spawn_worker(self):
        log.debug("test_spawn_worker with ds_id: %s" % self.ncom_ds_id)
        proc_name, pid, queue_id = self.eoas_cli.spawn_worker(self.ncom_ds_id)
        log.debug("proc_name: %s\tproc_id: %s\tqueue_id: %s" % (proc_name, pid, queue_id))
        self.assertEquals(proc_name, self.ncom_ds_id+'_worker')

    @unittest.skip("Underlying method not yet implemented")
    def test_get_worker(self, resource_id=''):
        log.debug("test_spawn_worker with ds_id: %s" % self.ncom_ds_id)
        proc_name, pid, queue_id = self.eoas_cli.spawn_worker(self.ncom_ds_id)
        log.debug("proc_name: %s\tproc_id: %s\tqueue_id: %s" % (proc_name, pid, queue_id))

        with self.assertRaises(IonException):
            self.eoas_cli.get_worker(resource_id=self.ncom_ds_id)

    @unittest.skip("Underlying method not yet implemented")
    def test_get_capabilities(self):
        log.debug("test_spawn_worker with ds_id: %s" % self.ncom_ds_id)
        proc_name, pid, queue_id = self.eoas_cli.spawn_worker(self.ncom_ds_id)
        log.debug("proc_name: %s\tproc_id: %s\tqueue_id: %s" % (proc_name, pid, queue_id))

        with self.assertRaises(IonException):
            self.eoas_cli.get_capabilities()

#    @unittest.skip("")
    def test_execute_single_worker(self):
        ds_id = self.ncom_ds_id

        log.debug("test_spawn_worker with ds_id: %s" % ds_id)
        proc_name, pid, queue_id = self.eoas_cli.spawn_worker(ds_id)
        log.debug("proc_name: %s\tproc_id: %s\tqueue_id: %s" % (proc_name, pid, queue_id))

#        with self.assertRaises(IonException):
#            self.eoas_cli.execute()

        cmd = AgentCommand(command_id="111", command="get_attributes", kwargs={"ds_id":ds_id})
        log.debug("Execute AgentCommand: %s" % cmd)
        ret = self.eoas_cli.execute(command=cmd)
        log.debug("Returned: %s" % ret)
        self.assertEquals(ret.status, "SUCCESS")
        self.assertTrue(type(ret.result[0]), dict)

        cmd = AgentCommand(command_id="112", command="get_signature", kwargs={"ds_id":ds_id})
        log.debug("Execute AgentCommand: %s" % cmd)
        ret = self.eoas_cli.execute(command=cmd)
        log.debug("Returned: %s" % ret)
        self.assertEquals(ret.status, "SUCCESS")
        self.assertTrue(type(ret.result[0]), dict)

#    @unittest.skip("")
    def test_execute_multi_worker(self):
        log.debug("test_spawn_worker #1 with ds_id: %s" % self.ncom_ds_id)
        proc_name, pid, queue_id = self.eoas_cli.spawn_worker(self.ncom_ds_id)
        log.debug("proc_name: %s\tproc_id: %s\tqueue_id: %s" % (proc_name, pid, queue_id))

        log.debug("test_spawn_worker #2 with ds_id: %s" % self.hfr_ds_id)
        proc_name, pid, queue_id = self.eoas_cli.spawn_worker(self.hfr_ds_id)
        log.debug("proc_name: %s\tproc_id: %s\tqueue_id: %s" % (proc_name, pid, queue_id))

        cmd = AgentCommand(command_id="112", command="get_signature", kwargs={"ds_id":self.ncom_ds_id})
        log.debug("Execute AgentCommand: %s" % cmd)
        ret = self.eoas_cli.execute(command=cmd)
        log.debug("Returned: %s" % ret)
        self.assertEquals(ret.status, "SUCCESS")
        self.assertTrue(type(ret.result[0]), dict)

        cmd = AgentCommand(command_id="112", command="get_signature", kwargs={"ds_id":self.hfr_ds_id})
        log.debug("Execute AgentCommand: %s" % cmd)
        ret = self.eoas_cli.execute(command=cmd)
        log.debug("Returned: %s" % ret)
        self.assertEquals(ret.status, "SUCCESS")
        self.assertTrue(type(ret.result[0]), dict)

#    @unittest.skip("")
    def test_execute_acquire_data(self):
        ds_id = self.ncom_ds_id

        log.debug("test_spawn_worker with ds_id: %s" % ds_id)
        proc_name, pid, queue_id = self.eoas_cli.spawn_worker(ds_id)
        log.debug("proc_name: %s\tproc_id: %s\tqueue_id: %s" % (proc_name, pid, queue_id))

        cmd = AgentCommand(command_id="113", command="acquire_data", kwargs={"ds_id":ds_id})
        log.debug("Execute AgentCommand: %s" % cmd)
        ret = self.eoas_cli.execute(command=cmd)
        log.debug("Returned: %s" % ret)
        self.assertEquals(ret.status, "SUCCESS")

    @unittest.skip("Underlying method not yet implemented")
    def test_set_param(self):
        log.debug("test_spawn_worker with ds_id: %s" % self.ncom_ds_id)
        proc_name, pid, queue_id = self.eoas_cli.spawn_worker(self.ncom_ds_id)
        log.debug("proc_name: %s\tproc_id: %s\tqueue_id: %s" % (proc_name, pid, queue_id))

        with self.assertRaises(IonException):
            self.eoas_cli.set_param(name="param", value="value")

    @unittest.skip("Underlying method not yet implemented")
    def test_get_param(self):
        log.debug("test_spawn_worker with ds_id: %s" % self.ncom_ds_id)
        proc_name, pid, queue_id = self.eoas_cli.spawn_worker(self.ncom_ds_id)
        log.debug("proc_name: %s\tproc_id: %s\tqueue_id: %s" % (proc_name, pid, queue_id))

        with self.assertRaises(IonException):
            self.eoas_cli.get_param(name="param")

    @unittest.skip("Underlying method not yet implemented")
    def test_execute_agent(self):
        log.debug("test_spawn_worker with ds_id: %s" % self.ncom_ds_id)
        proc_name, pid, queue_id = self.eoas_cli.spawn_worker(self.ncom_ds_id)
        log.debug("proc_name: %s\tproc_id: %s\tqueue_id: %s" % (proc_name, pid, queue_id))

        with self.assertRaises(IonException):
            self.eoas_cli.execute_agent()

    @unittest.skip("Underlying method not yet implemented")
    def test_set_agent_param(self):
        log.debug("test_spawn_worker with ds_id: %s" % self.ncom_ds_id)
        proc_name, pid, queue_id = self.eoas_cli.spawn_worker(self.ncom_ds_id)
        log.debug("proc_name: %s\tproc_id: %s\tqueue_id: %s" % (proc_name, pid, queue_id))

        with self.assertRaises(IonException):
            self.eoas_cli.set_agent_param(name="param", value="value")

    @unittest.skip("Underlying method not yet implemented")
    def test_get_agent_param(self):
        log.debug("test_spawn_worker with ds_id: %s" % self.ncom_ds_id)
        proc_name, pid, queue_id = self.eoas_cli.spawn_worker(self.ncom_ds_id)
        log.debug("proc_name: %s\tproc_id: %s\tqueue_id: %s" % (proc_name, pid, queue_id))

        with self.assertRaises(IonException):
            self.eoas_cli.get_agent_param(name="param")
