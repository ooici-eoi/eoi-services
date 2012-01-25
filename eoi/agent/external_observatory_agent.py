#!/usr/bin/env python

"""
@package 
@file external_observatory_agent_instance
@author Christopher Mueller
@brief 
"""
from interface.objects import AgentCommandResult, AgentCommand

__author__ = 'Christopher Mueller'
__licence__ = 'Apache 2.0'

from pyon.public import log, AT, RT
from pyon.agent.agent import ResourceAgent
from pyon.ion.endpoint import StreamPublisher
from eoi.agent.handler.base_external_data_handler import BaseExternalDataHandler
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient


class ExternalObservatoryAgent(ResourceAgent):

    _data_handler = None
    _stream_publisher = None

    def __init__(self, *args, **kwargs):
        ResourceAgent.__init__(self, *args, **kwargs)

    def _on_init(self):
        ResourceAgent._on_init(self)

#        log.debug(">>funcs:\n%s" % dir(self))
#        log.debug(">>type:\n%s" % type(self))
#        log.debug(">>CFG:\n%s" % self.CFG)
#        log.debug(">>_proc_type:\n%s" % self._proc_type)

        ext_dataset_id = self.CFG.get('process',{}).get('eoa',{}).get('dataset_id','')
        log.debug(">>ds_id:\n\t%s" % ext_dataset_id)
        self.resreg_cli = ResourceRegistryServiceClient()
        self.pubsub_cli = PubsubManagementServiceClient()


        # retrieve all the ExternalDataset resource associated resources
        ext_ds_res = self.resreg_cli.read(object_id=ext_dataset_id)
        log.debug("Retrieved ExternalDataset: %s" % ext_ds_res)

        dsrc_res, dsrc_acc = self.resreg_cli.find_objects(subject=ext_dataset_id, predicate=AT.hasSource, object_type=RT.DataSource)
        dsrc_res = dsrc_res[0]
        dsrc_id = dsrc_acc[0][3]
        log.debug("Found associated DataSource: %s" % dsrc_id)

        edp_res, edp_acc = self.resreg_cli.find_objects(subject=dsrc_id, predicate=AT.hasProvider, object_type=RT.ExternalDataProvider)
        edp_res = edp_res[0]
        edp_id = edp_acc[0][3]
        log.debug("Found associated ExternalDataProvider: %s" % edp_id)

        mdl_res, mdl_acc = self.resreg_cli.find_objects(subject=dsrc_id, predicate=AT.hasModel, object_type=RT.ExternalDataSourceModel)
        mdl_res = mdl_res[0]
        mdl_id = mdl_acc[0][3]
        log.debug("Found associated ExternalDataSourceModel: %s" % mdl_id)

        stream_id, _ = self.resreg_cli.find_objects(subject=ext_dataset_id, predicate=AT.hasStream, object_type=RT.Stream, id_only=True)
        stream_id = stream_id[0]
        log.debug("Found associated Stream: %s" % stream_id)

        # configure the stream publisher
        log.debug("Configure StreamPublisher")
        stream_route = self.pubsub_cli.read_stream(stream_id=stream_id)
        log.debug("StreamRoute: %s" % stream_route)

        self._stream_publisher = StreamPublisher(stream_route)

        # instantiate the data_handler instance
        modpath = mdl_res.data_handler_module
        log.debug("ExternalDataHandler module: %s" % modpath)
        classname = mdl_res.data_handler_class
        log.debug("ExternalDataHandler class: %s" % classname)
        module = __import__(modpath, fromlist=[classname])
        classobj = getattr(module, classname)
        self._data_handler = classobj(data_provider=edp_res, data_source=dsrc_res, ext_dataset=ext_ds_res)
        assert isinstance(self._data_handler, BaseExternalDataHandler), "Instantiated service not a BaseExternalDataHandler %r" % self._data_handler

    def _on_quit(self):
        self._data_handler.close()
        ResourceAgent._on_quit(self)

    def execute(self, command=None):
        log.debug("execute (worker): command=%s" % command)
        ret = AgentCommandResult(command_id=command.command_id, command=command.command)
        if command is not None:
            cmd_str = command.command
            if cmd_str == "get_attributes":
                try:
                    ret.result = [self._data_handler.get_attributes()]
                    ret.status = "SUCCESS"
                except Exception as ex:
                    ret.status = "ERROR"
                    ret.result = [ex]
            elif cmd_str == "get_signature":
                try:
                    ret.result = [self._data_handler.get_signature()]
                    ret.status = "SUCCESS"
                except Exception as ex:
                    ret.result = [ex]
                    ret.status = "ERROR"
            elif cmd_str is "acquire_data":
                try:
                    pass
                except Exception as ex:
                    ret.result = [ex]
                    ret.status = "ERROR"
                pass
            else:
                ret.status = "UNKNOWN COMMAND"
        else:
            ret.status = "AgentCommand is None"

        return ret




