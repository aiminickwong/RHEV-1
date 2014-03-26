#!/usr/bin/env python
#-*- coding:utf-8 -*-

import optparse
import ovirtsdk.api
import config
import pg
import pexpect
import time

from time import sleep
from ovirtsdk.xml import params
from ovirtsdk.infrastructure import errors
from ovirtsdk.infrastructure import contextmanager
from ovirtsdk.infrastructure.brokers import VMSnapshot
from ovirtsdk.infrastructure.brokers import VMSnapshots

from utils import Utils
from cmcc_restore import RedhatCmccRestore
from cmcc_snaplist import RedhatCmccSnapMap
from cmcc_delete import RedhatCmccDelete
import os               # Miscellaneous OS interfaces.
import sys              # System-specific parameters and functions.

PREFIX = config.PREFIX_FOR_VMNAME
internalSnapName = "_.internal"

# Default daemon parameters.
# File mode creation mask of the daemon.
UMASK = 0

# Default working directory for the daemon.
WORKDIR = "/"

MAXFD = 1024

try:
    if config.REDIRECT_TO:
        REDIRECT_TO = config.REDIRECT_TO
except:
    REDIRECT_TO = None
    pass


class SnapshotHandler(object):

    def __init__(self, enMemery='0'):
        self.api = self.ssh_get_api()
        self.db = self.ssh_get_db()
        self.enMemery = str(enMemery)


    def ssh_get_api(self):
        """
	"""
        api = ovirtsdk.api.API( 
                  url=config.OVIRT_URL, 
                  insecure=True,
                  username=config.OVIRT_USERNAME+'@'+config.OVIRT_DOMAIN,
                  password=config.OVIRT_PASSWORD,
                  ca_file=config.OVIRT_CA_FILE,)

	return api


    def ssh_get_db(self):
        """
	"""
        conn = pg.connect( 
                   dbname=config.OVIRT_DB_NAME,
                   host=config.OVIRT_DB_HOST,
		   user=config.OVIRT_DB_USER,
		   passwd=config.OVIRT_DB_PASSWORD)

	return conn


    def ssh_get_vmList(self):
        """
	"""
	vmList = self.api.vms.list()
	return vmList


    def ssh_get_vmObj(self, vmName):
        """
	"""
	for vm in self.ssh_get_vmList():
       	    if vm.get_name() == vmName:
                return vm
        return None


    def ssh_get_vmID(self, vmName):
        vmObj = self.ssh_get_vmObj(vmName)
        return vmObj.get_id()


    def ssh_get_snapObjList(self, vmObj):
        """
	"""
        if vmObj:
            #print "SSSSSSSSSS",vmObj
	    snapObjList = vmObj.get_snapshots().list()
	    return snapObjList
        return []


    def ssh_get_snapName(self, snapObj):
        """
        """
        return snapObj.get_description()


    def ssh_get_snapList(self, vmObj):
        """
        """
        snapObjList = self.ssh_get_snapObjList(vmObj)
        return [self.ssh_get_snapName(snapObj) for snapObj in snapObjList ]


    def ssh_get_snapObj(self, vmName, snapName):
        """
        """
        vmObj = self.ssh_get_vmObj(vmName )
        for snapObj in self.ssh_get_snapObjList(vmObj):
            if self.ssh_get_snapName(snapObj) == snapName:
                return snapObj
        return None

    def rcr_wait_event(self, event_txt):
        retry = 300
        while (retry > 0):
            time.sleep(10)
            retry = retry - 1
            api = 'events'
            eventsInfo = Utils().curl_get_method(api)
            ioBuffer = Utils().xml_to_ioBuffer(eventsInfo)
            root = ioBuffer.documentElement
            event_nodes = root.getElementsByTagName('event')
            for event_node in event_nodes:
                event_desc_node = event_node.getElementsByTagName('description')[0]
                event_desc = Utils().xml_get_text(event_desc_node.childNodes).strip()
                if event_desc == event_txt:
                    return 1
        return 0

    def ssh_create_snap(self, vmName, snapName):
        """
        """
        snapNum = len(self.ssh_get_vmObj(vmName).snapshots.list())
        print snapNum
        if snapNum == 1:  
            enMemery = self.enMemery
            self.enMemery = 0
            self._ssh_create_snap(vmName, internalSnapName)
            #Snapshot '_.internal' creation for VM 'vm2' has been completed.
            self.rcr_wait_event("Snapshot '%s' creation for VM '%s' has been completed." % (internalSnapName,vmName))
            self.enMemery = enMemery
            snap_uuid = self._ssh_create_snap(vmName, snapName)
            return snap_uuid
        return self._ssh_create_snap(vmName, snapName)
        
    
    def _ssh_create_snap(self, vmName, snapName):
        """
        """
        vmObj = self.ssh_get_vmObj(vmName)
        snapList = self.ssh_get_snapList(vmObj)
        if snapName in snapList:
            return ' [e] Error snapshotName alreay existed.'

        apiID = self.api.id
        vss = VMSnapshots(vmObj, apiID)
        if self.enMemery == '1':
            snapshotParams = params.Snapshot(description=snapName,
                                             persist_memorystate=True)
        else:
            snapshotParams = params.Snapshot(description=snapName,
                                             persist_memorystate=False)
        #[@param snapshot.persist_memorystate: boolean]

        vmState = vmObj.status.state 
        # create snapshot.
        vss.add( snapshot=snapshotParams )
        if not vmState == 'up':
            print 'creating snaphot: %s' % snapName
            return 'Snapshot %s created.' % snapName
 	retry = 6000
        while (retry > 0):
            sleep(1)
            snapObj = self.ssh_get_snapObj(vmName, snapName)
            if not snapObj:
                continue
            if snapObj.get_snapshot_status() == 'ok':
                print 'finished...'
                break
            retry = retry -1

        if not vmObj.status.state == 'up':
            vmObj.start()

        print 'Snapshot %s created.' % snapName
        return snapObj.get_id()


    def ssh_restore_snap(self, vmName, snapName):
        """
        """
        if REDIRECT_TO:
            child_pid = os.fork()
            if child_pid == 0:
                os.setsid()
                print "Child Process: PID# %s" % os.getpid()
                try:
                    pid = os.fork()	# Fork a second child.
                except OSError, e:
                    raise Exception, "%s [%d]" % (e.strerror, e.errno)
                if (pid == 0):
                    os.chdir(WORKDIR)
                    os.umask(UMASK)
                else:
                    os._exit(0)
            else:
                print "Parent Process: PID# %s" % os.getpid()
                os._exit(0)

        if REDIRECT_TO:
            maxfd = MAXFD
            
            for fd in range(0, maxfd):
                try:
                    os.close(fd)
                except OSError:	# ERROR, fd wasn't open to begin with (ignored)
                    pass

            os.open(REDIRECT_TO, os.O_RDWR|os.O_CREAT)	# standard input (0)

            # Duplicate standard input to standard output and standard error.
            os.dup2(0, 1)			# standard output (1)
            os.dup2(0, 2)			# standard error (2)

        rcs = RedhatCmccSnapMap(self.api)
        snapMap = rcs.rcs_get_snapMap(vmName)
        snapInfo = snapMap.get(snapName)
        realVmName = snapInfo.get('vmName')
        print 'realVmName: ',realVmName

        rcr = RedhatCmccRestore()
        vmObj = self.ssh_get_vmObj(vmName)
        vmID = self.ssh_get_vmID(vmName)
        print 'vmname', vmName
        print vmObj.status.state
        if vmObj.status.state == 'up':
            #vmObj.shutdown()
            print "we need poweroff current vm before we restore it from snapshot"
            vmObj.stop()
        #we need wait current vm been poweroff
        #TODO
        print 'vmObj.status.state = ',vmObj.status.state
        while vmObj.status.state != 'down':
            try:
                vmObj.stop()
            except:
                pass
            vmObj = self.ssh_get_vmObj(vmName)
            print 'vmObj.status.state = ',vmObj.status.state
            time.sleep(10)
        #TODO

        snapMap = rcs.rcs_get_snapMap(vmName)

        if snapMap.keys()[-1] == snapName:
            vmObj = self.ssh_get_vmObj(vmName)
            for snapObj in vmObj.get_snapshots().list():
                if snapObj.get_description() == 'Active VM':
                    continue
                if snapObj.get_description() == snapName:
                    snapshotParams = params.Action(restore_memory=True)
                    snapObj.restore(action=snapshotParams)
                    vmObj.start()

            os._exit(0)
        (vmID_new,vmID,snapID) = rcr.rcr_create_vm_by_snap(realVmName, snapName)
        print 'vmID_new: ',vmID_new
        print 'vmID: ', vmID
        print 'snapID: ', snapID
        rcr.rcr_swap_mic(vmID_new,vmID)
        rcr.rcr_swap_name(vmID_new,vmID)
        rcr.rcr_restore_with_memory_snap(vmID, vmID_new,snapID)
        print 'All task done!!!'


    def ssh_list_snap(self, vmName, snapName):
        """
        """
        rcsm = RedhatCmccSnapMap(self.api)
        snapMap = rcsm.rcs_get_snapMap(vmName)
        snapList = sorted(snapMap.keys())
        return snapList

        
    def _ssh_delete_snap(self, vmName, snapName):
        """
        """
        rcd = RedhatCmccDelete(self.api)
        vmName = vmName
        snapName = snapName
        rcd.rcd_delete_snap(vmName, snapName)


    def ssh_delete_snap(self, vmName, snapName):
        """
        """
        rcs = RedhatCmccSnapMap(self.api)
        snapMap = rcs.rcs_get_snapMap(vmName)
        snapInfo = snapMap.get(snapName)
        realVmName = snapInfo.get('vmName')
        print 'realVmName: ',realVmName
        if realVmName.startswith(PREFIX):
            vmName = realVmName
            vmObj = self.ssh_get_vmObj(vmName)
            vmID = self.ssh_get_vmID(vmName)
            print 'vmname', vmName
            print vmObj.status.state
            if vmObj.status.state == 'up':
                vmObj.stop()
            print 'vmObj.status.state = ',vmObj.status.state
            while vmObj.status.state != 'down':
                try:
                    vmObj.stop()
                except:
                    pass
                vmObj = self.ssh_get_vmObj(vmName)
                print 'vmObj.status.state = ',vmObj.status.state
                time.sleep(10)
            vmObj = self.ssh_get_vmObj(vmName)
            for snapObj in vmObj.get_snapshots().list():
                if snapObj.get_description() == 'Active VM':
                    continue
                if snapObj.get_description() == snapName:
                    snapObj.delete()
        else:
            vmObj = self.ssh_get_vmObj(vmName)
            vmID = self.ssh_get_vmID(vmName)
            print 'vmname', vmName
            print vmObj.status.state
            if vmObj.status.state == 'up':
                self._ssh_delete_snap(vmName,snapName)
            else:
                vmObj = self.ssh_get_vmObj(vmName)
                for snapObj in vmObj.get_snapshots().list():
                    if snapObj.get_description() == 'Active VM':
                        continue
                    if snapObj.get_description() == snapName:
                        snapObj.delete()
    
    def ssh_cleanup(self):
        """
        """
        rcsm = RedhatCmccSnapMap(self.api)
        snapMap = rcsm.rcs_get_snapMap(vmName)
        snapList = sorted(snapMap.keys())
        return snapList

        

def get_option():
        parser = optparse.OptionParser()
        parser.add_option('-u', action='store', dest='hostInfo',
                      help='root@10.41.5.31@123456')
        parser.add_option('-o', action='store', dest='option',
                                 help="Input: 'list': list all snapshots for vm \
                                 or 'create': create a new snapshot for vm \
                                 or 'delete': delete the snapshot with snapshotName\
                                 or 'restore': restore vm using the snapshot \
                                 or 'cleanup': clean up _. vms")
        parser.add_option('-v', action='store', dest='vmName')
        parser.add_option('-s', action='store', dest='snapName')
        parser.add_option('-m', action='store', dest='enMemery')
        (opts, args) = parser.parse_args()
        return (opts, args)


if __name__ == '__main__':
    opts, args = get_option()
    hostInfo = opts.hostInfo
    option = opts.option
    vmName = opts.vmName
    snapName = opts.snapName
    enMemery = opts.enMemery

    if option: 
        ssh = SnapshotHandler(enMemery)
        vm_name = vmName
        snap_name = snapName

        acitonDict = {'create': ssh.ssh_create_snap,
                  'list': ssh.ssh_list_snap,
                  'delete': ssh.ssh_delete_snap,
                  'restore': ssh.ssh_restore_snap,
                  'cleanup': ssh.ssh_cleanup,}

        print hostInfo, option, vmName, snapName
        print acitonDict[option.lower()](vm_name, snap_name)
     
    
