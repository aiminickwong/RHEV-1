#!/usr/bin/env python
#-*- File : multiVmCreate -*-
#-*- Auth : jfyang -*-
#-*- Email: jfyang@ronglian.com -*-
# Copyright: United Electronics Co., Ltd.(UEC)

import uuid
import config
import logging
import optparse
import threading
from time import sleep
from Queue import Queue

from ovirtsdk.api import API as _API
from ovirtsdk.utils.parsehelper import ParseHelper
from ovirtsdk.xml import params
from ovirtsdk.infrastructure.brokers import VMs as _VMs
from ovirtsdk.infrastructure.brokers import VM
from ovirtsdk.infrastructure.context import context
from ovirtsdk.infrastructure.errors import DisconnectedError

logging.basicConfig(level=logging.DEBUG,
                    format='%(message)s',)

class API(_API):
    
    def __init__(self, *args, **kw):
        _API.__init__(self, *args, **kw)
        self.vms = VMs(self.id)

class VMs(_VMs):

    def __init__(self, *args, **kw):
        _VMs.__init__(self, *args, **kw)

    def __getProxy(self):
        proxy = context.manager[self.context].get('proxy')
        if proxy:
            return proxy
        raise DisconnectedError

    def add(self, vm, correlation_id=None, expect=None):
        url = '/api/vms'
        result = self.__getProxy().add(
                url=url,
                body=ParseHelper.toXml(vm),
                headers={"Correlation-Id":correlation_id, "Expect":expect})
       
        return VM(result, self.context)

class VmParamsQueue(Queue):

    def __init__(self):
        Queue.__init__(self) 
        
    def _put(self, item):
        self.queue.append(item)

class VmParamsQueueHandler(threading.Thread):

    def __init__(self, api, event, vmParamsQueue, retValQueue, maxVmNum=1):
        threading.Thread.__init__(self)
        self.api = api
        self.event = event
        self.vmParamsQueue = vmParamsQueue
        self.retValQueue = retValQueue
        self.maxVmNum = maxVmNum
    
    def run(self):
        logging.debug('%s Starting' % threading.currentThread().getName())
        self.event.wait() 
        while not self.vmParamsQueue.empty() and self.maxVmNum:
            self.maxVmNum -= 1
            vm_params = self.vmParamsQueue.get()
            try:
                ret = self.api.vms.add(vm=vm_params.get('paramsVM'))
                self.retValQueue.put(ret)
                logging.debug("Virtual machine '%s' added." % vm_params['vm_name'])
            except Exception as ex:
                logging.debug("Adding virtual machine '%s' failed: %s"%(
                                                            vm_params['vm_name'],ex))
        logging.debug('%s Exiting' % threading.currentThread().getName())

class MultiVmCreate(object):

    def __init__(self, **kw):
        self.vmNum    = kw.get('vmNum')
        self.vmName   = kw.get('vmName')
        self.cluster  = kw.get('cluster')
        self.template = kw.get('template')
        self.api = API(url=config.OVIRT_URL,
                       username=config.OVIRT_USERNAME+'@'+config.OVIRT_DOMAIN,
                       password=config.OVIRT_PASSWORD,
                       ca_file=config.OVIRT_CA_FILE)
        logging.debug('self.api: %s'% self.api )
        self.maxVmNum = 1
        self.vmParamsQueue = VmParamsQueue()
        self.retValQueue = VmParamsQueue()
        self._syncCreateEvent = threading.Event()

    def _vmParamsQueueHandlerThread(
                            self,api,event,
                            vmParamsQueue,
                            retValQueue,
                            maxVmNum=1):
        t = VmParamsQueueHandler(
                            api,event,
                            vmParamsQueue,
                            retValQueue,
                            maxVmNum)
        t.start()

    def _getVmList(self):
        vmList = []
        for vm in self.api.vms.list():
            if vm.get_name().startswith(self.vmName):
                vmList.append(vm)
        return vmList

    def _addParams(self, vmParamsQueue, vmNum):
        vm_cluster = self.api.clusters.get(name=self.cluster)
        vm_template = self.api.templates.get(name=self.template)
        vmList = self._getVmList()
        length = len(vmList)
        for i in range(vmNum):
            vm_name = self.vmName + str(i+1+length)
            paramsVM = params.VM(name=vm_name,
                             cluster=vm_cluster,
                             template=vm_template,)
            vm_params = {}
            vm_params.setdefault('vm_name', vm_name)
            vm_params.setdefault('paramsVM', paramsVM)
            vmParamsQueue.put(vm_params)

    def multiVmCreate(self):
        try:
            self._addParams(self.vmParamsQueue, self.vmNum)
            if self.vmNum % self.maxVmNum:
                threadNum = self.vmNum/self.maxVmNum + 1
            else:
                threadNum = self.vmNum/self.maxVmNum

            for i in range(threadNum):
                self._vmParamsQueueHandlerThread(self.api,
                                       self._syncCreateEvent,
                                       self.vmParamsQueue,
                                       self.retValQueue,
                                       self.maxVmNum,)
            self._syncCreateEvent.set()  
        except Exception as ex:
            logging.debug('Unexpected error: %s' % ex)
        while not self.retValQueue.qsize() == self.vmNum:
            sleep(1)
        self.api.disconnect()

def get_option():
    #vmName,cluster,template,vmNum
    parser = optparse.OptionParser()
    parser.add_option('-v', action='store', dest='vmName')
    parser.add_option('-t', action='store', dest='template')
    parser.add_option('-c', action='store', dest='cluster')
    parser.add_option('-n', action='store', dest='vmNum')
    (opts, args) = parser.parse_args()
    return (opts, args)

if __name__ == '__main__':
    opts, args = get_option() 
    vmName = opts.vmName
    template = opts.template
    cluster = opts.cluster
    vmNum = int(opts.vmNum)
    print vmName, template, cluster, vmNum

    mvc = MultiVmCreate(
            vmName=vmName,
            template=template,
            cluster=cluster,
            vmNum=vmNum,)
    mvc.multiVmCreate()



