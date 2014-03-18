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
import os               # Miscellaneous OS interfaces.
import sys              # System-specific parameters and functions.

PREFIX = config.PREFIX_FOR_VMNAME

# Default daemon parameters.
# File mode creation mask of the daemon.
UMASK = 0

# Default working directory for the daemon.
WORKDIR = "/"

MAXFD = 1024

if config.REDIRECT_TO:
    REDIRECT_TO = config.REDIRECT_TO

class SnapshotHandler(object):

    def __init__(self):
        self.api = self.ssh_get_api()
        self.db = self.ssh_get_db()

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
	snapObjList = vmObj.get_snapshots().list()
	return snapObjList

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
         
    
    def ssh_create_snap(self, vmName, snapName):
        """
        """
        vmObj = self.ssh_get_vmObj(vmName)
        snapList = self.ssh_list_snap(vmName, snapName)
        if snapName in snapList:
            return ' [e] Error snapshotName alreay existed.'

        apiID = self.api.id
        vss = VMSnapshots(vmObj, apiID)
        snapshotParams = params.Snapshot(description=snapName)

        vmState = vmObj.status.state 
        # create snapshot.
        vss.add( snapshot=snapshotParams )
        if not vmState == 'up':
            print 'creating snaphot: %s' % snapName
            return 'Snapshot %s created.' % snapName
 	retry = 60
        while (retry > 0):
            print 'creating snaphot: %s' % snapName
            print '...',
            sleep(5)
            snapObj = self.ssh_get_snapObj(vmName, snapName)
            if not snapObj:
                continue
            print snapObj.get_snapshot_status()
            if snapObj.get_snapshot_status() == 'ok':
                print 'finished...'
                break
            retry = retry -1

        if not vmObj.status.state == 'up':
            vmObj.start()

        return 'Snapshot %s created.' % snapName


    def ssh_restore_snap(self, vmName, snapName):
        """
        """
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
            print "正在恢复快照请耐心等待" 
            os._exit(0)

        if config.REDIRECT_TO:
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

    def getSnapObjByName(self,vm, snapName):
        for snap in vm.get_snapshots().list():
            if snap.get_description() == snapName:
                return snap

    def _callDbCmd(self, sqlcmd):
                return self.db.query(sqlcmd)

    def _imageTableHandler(self, snapID):
                sqlcmd = "select * \
                                  from images \
                                  where vm_snapshot_id='%s'"%snapID
                ret = self._callDbCmd(sqlcmd).dictresult()[0]
                volumeID = ret.get('image_guid')
                parentID = ret.get('parentid')
                imageID = ret.get('image_group_id')
                return (volumeID, parentID, imageID)
    def _getVolId(self, snapID):
                return self._imageTableHandler(snapID)[0]

    def _getVolParentId(self, snapID):
                return self._imageTableHandler(snapID)[1]

    def _getImageID(self, snapID):
                return self._imageTableHandler(snapID)[2]

    def _getDomainID(self, volumeID):
                sqlcmd = "select * \
                                  from image_storage_domain_map \
                                  where image_id='%s'" % volumeID
                ret = self._callDbCmd(sqlcmd).dictresult()[0]
                return ret.get('storage_domain_id')

    def _getPoolID(self, domainID):
                sqlcmd = "select storage_pool_id        \
                                  from storage_pool_iso_map     \
                                  where storage_id='%s'" % domainID
                ret = self._callDbCmd(sqlcmd).dictresult()[0]
                return ret.get('storage_pool_id')

    def _delSnapDstDir(self, snap_dst_path):
                cmd = 'rm -rf %s' % snap_dst_path
                ret = self._callBackendCmd(cmd)
                print ret

    def _getActiveVolId(self,vm):
                actSnapID = ''
                for snap in vm.snapshots.list():
                        if snap.get_description() == 'Active VM':
                                actSnapID = snap.get_id()
                actVolID,_,_ = self._imageTableHandler(actSnapID)
                return actVolID

    def _getHostInfo(self):
        #'root@10.41.5.31@123456'
        # return tuple(self.hostInfo.split('@'))
        return ('root',self.rhevhIp,config.OVIRT_PASSWORD)

    def _callBackendCmd(self, cmd):
                user,ip,password = self._getHostInfo()
                ret = ''
                print 'call cmd: %s' % cmd

                ssh = pexpect.spawn('ssh %s@%s "%s"'%(user,ip,cmd), timeout=330)
                try:
                        expect = ssh.expect(['password', 'continue connecting (yes/no)?'])
                        if expect == 0:
                                ssh.sendline(password)
                        elif expect == 1:
                                ssh.sendline('yes')
                                try:
                                        expect = ssh.expect(['password'])
                                        if expect == 0:
                                                ssh.sendline(password)
                                        else:
                                                print ' [e] expect error.'
                                except  pexpect.EOF:
                                        ssh.close()
                        else:
                                pass
                except pexpect.EOF:
                        ssh.close()
                else:
                        ret = ssh.read()
                        ssh.expect(pexpect.EOF)
                        ssh.close()
                return ret

    def _deleteSnapFromBackend(self, active_path, vmID, volumeID):
                cmd = 'python /usr/share/vdsm/volumeBackendHandler.py delete %s %s %s'%(
                                                                                        active_path,vmID, volumeID, )
                print 'cmd:%s' % cmd
                ret = self._callBackendCmd(cmd)
                return ret

    def _deleteSnapFromDb(self, snapID, volumeID, parentID):
                cmd1 = "delete from image_storage_domain_map \
                                a where a.image_id='%s'"%volumeID
                cmd2 = "delete from disk_image_dynamic \
                                a where a.image_id='%s'"%volumeID
                cmd3 = "delete from snapshots \
                                a where a.snapshot_id='%s'"%snapID
                cmd4 = "update images \
                                set parentid='%s' \
                                where parentid='%s'"%(parentID,volumeID)

                for cmd in [cmd1,cmd2,cmd3,cmd4]:
                        print cmd
                        self._callDbCmd(cmd)

    def liveDeleteSnapshot(self,vmName,snapName):
        """
        """
        vmID = self.ssh_get_vmID(vmName)
        vmObj = self.ssh_get_vmObj(vmName)
        snapID = self.getSnapObjByName(vmObj,snapName).get_id()
        print 'snapID: %s' % snapID
        volumeID, parentID, _ = self._imageTableHandler(snapID)
        imageID = self._getImageID(snapID)
        print 'imageID: %s' % imageID
        domainID = self._getDomainID(volumeID)
        print 'domainID: %s' % domainID
        poolID = self._getPoolID(domainID)
        print 'poolID: %s' % poolID
        actVolID = self._getActiveVolId(vmObj)
        print 'actVolID: %s' % actVolID
        active_path = ('/rhev/data-center/%s/%s/images/%s/%s')%(
                                                                                poolID,domainID,imageID,actVolID)
        print 'volumeID: %s' % volumeID
        print 'parentID: %s' % parentID
        print 'Deleting snapshot %s' % snapName
        hostObj = self.api.hosts.get(id=vmObj.host.id) 
        self.rhevhIp = hostObj.address
        print self._deleteSnapFromBackend(active_path, vmID, volumeID)
        self._deleteSnapFromDb(snapID, volumeID, parentID)
        print '...ok'
        print 'finished...'

    def ssh_delete_snap(self, vmName, snapName):
        """
        """
        print "正在删除快照请耐心等待"
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
                    print "正在删除快照请耐心等待..."
                    snapObj.delete()
        else:
            vmObj = self.ssh_get_vmObj(vmName)
            vmID = self.ssh_get_vmID(vmName)
            print 'vmname', vmName
            print vmObj.status.state
            if vmObj.status.state == 'up':
                print "!!!!!!在线删除功能只能支持一个硬盘的虚拟机!!!!!!!"
                print "正在进行非常危险的在线删除快照!!!!!\n请耐心等待..."
                self.liveDeleteSnapshot(vmName,snapName);
            else:
                vmObj = self.ssh_get_vmObj(vmName)
                for snapObj in vmObj.get_snapshots().list():
                    if snapObj.get_description() == 'Active VM':
                        continue
                    if snapObj.get_description() == snapName:
                        print "正在删除快照请耐心等待..."
                        snapObj.delete()
    

def get_option():
        parser = optparse.OptionParser()
        parser.add_option('-u', action='store', dest='hostInfo',
                      help='root@10.41.5.31@123456')
        parser.add_option('-o', action='store', dest='option',
                                 help="Input: 'list': list all snapshots for vm \
                                 or 'create': create a new snapshot for vm \
                                 or 'delete': delete the snapshot with snapshotName\
                                 or 'restore': restore vm using the snapshot")
        parser.add_option('-v', action='store', dest='vmName')
        parser.add_option('-s', action='store', dest='snapName')
        (opts, args) = parser.parse_args()
        return (opts, args)


if __name__ == '__main__':
    opts, args = get_option()
    hostInfo = opts.hostInfo
    option = opts.option
    vmName = opts.vmName
    snapName = opts.snapName
    print hostInfo, option, vmName, snapName

    ssh = SnapshotHandler()
    vm_name = vmName
    snap_name = snapName

    acitonDict = {'create': ssh.ssh_create_snap,
                  'list': ssh.ssh_list_snap,
                  'delete': ssh.ssh_delete_snap,
                  'restore': ssh.ssh_restore_snap, }
    print acitonDict[option.lower()](vm_name, snap_name)
     
    

