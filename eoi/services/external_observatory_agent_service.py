#!/usr/bin/env python

"""
@package 
@file ion/services/eoi/external_observatory_agent_service
@author Christopher Mueller
@brief 
"""

__author__ = 'Christopher Mueller'
__licence__ = 'Apache 2.0'

from mock import Mock
from pyon.net.endpoint import ProcessRPCClient
from pyon.public import log, IonObject, RT, AT
from pyon.core.exception import NotFound, IonException
from pyon.util.containers import DotDict
from interface.services.eoi.iexternal_observatory_agent_service import BaseExternalObservatoryAgentService
from interface.messages import external_observatory_agent_execute_in

class ExternalObservatoryAgentService(BaseExternalObservatoryAgentService):

    _worker_clients = {}

    def __init__(self):
        BaseExternalObservatoryAgentService.__init__(self)

        #TODO: de-mock
#        self.clients.process_dispatcher_service = DotDict()
#        self.clients.process_dispatcher_service["create_process_definition"] = Mock()
#        self.clients.process_dispatcher_service.create_process_definition.return_value = 'process_definition_id'
#        self.clients.process_dispatcher_service["schedule_process"] = Mock()
#        self.clients.process_dispatcher_service.schedule_process.return_value = True
#        self.clients.process_dispatcher_service["cancel_process"] = Mock()
#        self.clients.process_dispatcher_service.cancel_process.return_value = True
#        self.clients.process_dispatcher_service["delete_process_definition"] = Mock()
#        self.clients.process_dispatcher_service.delete_process_definition.return_value = True


    def spawn_worker(self, resource_id=''):
        '''
        Steps:
         1. check (with RR? with CEI??) to see if there is already a worker process for the resource_id (ExternalDataset Resource)
         2. retrieve the information necessary for the ExternalDataAgent to publish data (StreamPublisher?  DataProducer??  minimum of stream_id)
         3. spawn the worker process: currently via a "hack", eventually via CEI
         4. add the pid as the worker process for the resource_id
        '''
        log.debug("EOAS: spawn_worker")
        proc_name = resource_id+'_worker'
        log.debug("Process Name: %s" % proc_name)
        #todo: check if a process with this proc_name already exists, return that process

#        config = {'agent':{'dataset_id': resource_id}}
        config = {}
        config['process']={'name':proc_name,'type':'agent'}
        config['process']['eoa']={'dataset_id':resource_id}

        pid = self.container.spawn_process(name=proc_name, module='eoi.agent.external_observatory_agent', cls='ExternalObservatoryAgent', config=config)
#
#        return pid
        queue_id = "%s.%s" % (self.container.id, pid)

        self._worker_clients[resource_id] = ProcessRPCClient(name=queue_id, process=self)

        return proc_name, pid, queue_id

    def get_worker(self, resource_id=''):
        raise IonException

    def get_capabilities(self, capability_types=[]):
        raise IonException

    def execute(self, command=None):
        if command is not None:
            rid = command.ds_id
            log.debug("execute: res_id=%s" % rid)
            cli = self._worker_clients[rid]
            if cli is not None:
                exe = external_observatory_agent_execute_in(command=command)
                log.debug("execute: eoa_execute_in=%s" % exe)
                return cli.request(exe, op='execute')
        else:
            raise IonException


    def set_param(self, name='', value=''):
        raise IonException

    def get_param(self, name=''):
        raise IonException

    def execute_agent(self, command={}):
        raise IonException

    def set_agent_param(self, name='', value=''):
        raise IonException

    def get_agent_param(self, name=''):
        raise IonException