#!/usr/bin/env python
#-*- coding:utf-8 -*-
#########################################################################
#  #snapshotHandler -o delete -v vm -s snap2
#########################################################################

from utils import Utils
from xml.dom import minidom
from xml.dom.minidom import parseString as _domParseStr
import config
import xml.dom
import optparse
import time
import os
import ovirtsdk.api
import pg
import pexpect


from utils import Utils
PREFIX = config.PREFIX_FOR_VMNAME
internalSnapName = "_.internal"

def get_api():
    """
    """
    api = ovirtsdk.api.API(
              url=config.OVIRT_URL,
              insecure=True,
              username=config.OVIRT_USERNAME+'@'+config.OVIRT_DOMAIN,
              password=config.OVIRT_PASSWORD,
              ca_file=config.OVIRT_CA_FILE,)

    return api

from cmcc_snaplist import RedhatCmccSnapMap


def calc_time_wrap(func):

    def wrap(*args, **kwargs):
        begin_time = Utils().rpc_get_current_time()
 
        ret = func(*args, **kwargs)

        end_time = Utils().rpc_get_current_time()
        print '------'
        print func.func_name,'ueed time: ',
        print int(end_time)-int(begin_time)
        print '------'
        return ret

    return wrap

class RedhatCmccDelete(object):
    
    def __init__(self, api):
        self.api = api
        self.conn = self.rcd_get_db()

    def rcd_get_db(self):
        """
        """
        conn = pg.connect(
                   dbname=config.OVIRT_DB_NAME,
                   host=config.OVIRT_DB_HOST,
                   user=config.OVIRT_DB_USER,
                   passwd=config.OVIRT_DB_PASSWORD)

        return conn

    def rcd_delete_snap(self, vmName, snapName):
        """
        """
        vmObj = self.rcd_get_vmObj(vmName)
        vmID = vmObj.id

        snapID = self.rcd_get_snapID(vmName, snapName)
        snapVolumeID_list = self.rcd_get_volumeIDList(snapID)
        activeID = self.rcd_get_snapID(vmName, 'Active VM')
        activeVolumeID_list = self.rcd_get_volumeIDList(activeID)

        activeDisks = {}
        snapDisks = {}
        for activeVolumeID in activeVolumeID_list:
            cmd = "SELECT image_group_id from images where image_guid='%s'" % activeVolumeID
            image_group_id = self.rcd_call_dbCmd(cmd).dictresult()[0].get('image_group_id')
            cmd = "SELECT image_guid,parentid,vm_snapshot_id from images where image_group_id='%s'" % image_group_id
            diskVolumes = self.rcd_call_dbCmd(cmd).dictresult()
            activeDisks[image_group_id] = diskVolumes
        for snapVolumeID in snapVolumeID_list:
            cmd = "SELECT image_group_id from images where image_guid='%s'" % snapVolumeID
            image_group_id = self.rcd_call_dbCmd(cmd).dictresult()[0].get('image_group_id')
            cmd = "SELECT image_guid,parentid,vm_snapshot_id from images where image_group_id='%s'" % image_group_id
            diskVolumes = self.rcd_call_dbCmd(cmd).dictresult()
            snapDisks[image_group_id] = diskVolumes
        is_leaf = None
        for actDsk in activeDisks.keys():
            if snapDisks.has_key(actDsk):
                print '=3=>'*20
                for snapVol in snapDisks[actDsk]:
                    if snapVol['vm_snapshot_id'] == snapID:
                        print '=4=>'*20
                        print snapVol
                        # it is the close to templat snap
                        if snapVol['parentid'] == '00000000-0000-0000-0000-000000000000':
                            cmd = "update snapshots set description='%s' where snapshot_id='%s'" % (internalSnapName,snapID)
                            self.rcd_call_dbCmd(cmd)
                            return
                        cmd = "SELECT image_guid,parentid,vm_snapshot_id from images where parentid='%s'" % snapVol['image_guid']
                        pd = self.rcd_call_dbCmd(cmd).dictresult()
                        if len(pd) > 1:
                            cmd = "update snapshots set description='%s' where snapshot_id='%s'" % (internalSnapName,snapID)
                            self.rcd_call_dbCmd(cmd)
                            return
                        elif len(pd) == 1:
                            pID = pd[0].get('parentid');
                            cmd = "SELECT image_guid,parentid,vm_snapshot_id from images where parentid='%s'" % pID
                            pd2 = self.rcd_call_dbCmd(cmd).dictresult()
                            if len(pd2) > 1:
                                cmd = "update snapshots set description='%s' where snapshot_id='%s'" % (internalSnapName,snapID)
                                self.rcd_call_dbCmd(cmd)
                                return
                            for activeVol in activeDisks[actDsk]:
                                if activeVol['vm_snapshot_id'] == snapID:
                                    print 'vmID: ',
                                    print vmID
                                    print 'snapID: ',
                                    print snapID
                                    print "XXXXXX"*30
                                    #self.rcd_deleteSnap_fromBackend(vmName,vmID, snapID)
                                    #self.rcd_deleteSnap_memoryVolume(vmID, snapID)
                                    #self.rcd_deleteSnap_fromDb(snapID)
                                    #return
                                    cmd = "update snapshots set description='%s' where snapshot_id='%s'" % (internalSnapName,snapID)
                                    self.rcd_call_dbCmd(cmd)
                                    return
                            #TODO 
                            cmd = "update snapshots set description='%s' where snapshot_id='%s'" % (internalSnapName,snapID)
                            self.rcd_call_dbCmd(cmd)
                            return
                        else: # 
                            is_leaf = True  
                            cmd = "SELECT storage_domain_id from image_storage_domain_map where image_id='%s'" % snapVol['image_guid']
                            storageDomainID = self.rcd_call_dbCmd(cmd).dictresult()[0].get('storage_domain_id')
                            cmd = 'lvremove /dev/%s/%s' % (storageDomainID,snapVol['image_guid'])
                            print cmd
                            print self.rcd_call_backendCmd(cmd, vmID)

        if is_leaf:
            self.rcd_deleteSnap_memoryVolume(vmID, snapID)
            self.rcd_deleteSnap_fromDb(snapID)

    @calc_time_wrap
    def rcd_get_vmID(self, vmName):
        """
        """
        for vmObj in self.api.vms.list():
           if vmObj.get_name() == vmName:
               return vmObj.get_id()
        return None

    def rcd_get_vmObj(self, vmName):
        """
        """
        for vmObj in self.api.vms.list():
           if vmObj.get_name() == vmName:
               return vmObj
        return None
    
    @calc_time_wrap
    def rcd_get_snapID(self, vmName, snapName):
        """
        """
        for vmObj in self.api.vms.list():
            if vmObj.get_name() == vmName:
                for snapObj in vmObj.get_snapshots().list():
                    if snapObj.get_description() == snapName:
                        return snapObj.get_id()
        return None
 
    
    @calc_time_wrap
    def rcd_deleteSnap_fromBackend(self, vmName, vmID, snapID):
        """
        """
        
        disk = self.rcd_get_diskName(vmName, vmID)
        print 'disk keys: %s ' % disk

        for imageID in disk.keys():
            cmd = "SELECT image_guid from images where "
            cmd += "vm_snapshot_id='%s' and image_group_id='%s'" % (snapID, imageID)

            res = self.rcd_call_dbCmd(cmd).dictresult()
            if len(res) > 0 : 
                volumeID=res[0].get('image_guid')
                print '==>'*20
                print 'volumeID: ',
                print volumeID
                diskName = disk[imageID]

                cmd = 'python /usr/share/vdsm/mergeLivedVmChain.py %s %s %s'%(vmID,volumeID,diskName)
                print cmd
                print self.rcd_call_backendCmd(cmd, vmID)

    def rcd_deleteSnap_memoryVolume(self, vmID, snapID):
        """
        """
        memory_volume_list = self.rcd_get_memoryVolumeList(snapID)
        for memory_volume in memory_volume_list:
            cmd = 'lvremove %s' % memory_volume
            print cmd
            print self.rcd_call_backendCmd(cmd, vmID)


    def rcd_get_memoryVolumeList(self, snapID):
        """
        """
        cmd = "select memory_volume from snapshots where snapshot_id='%s'"%snapID
        res = self.rcd_call_dbCmd(cmd).dictresult()[0].get('memory_volume')
        memory_volume_list = []
        if res:
            resList = res.split(',')
            print '==>'*20
            print 'memory_volume: '
	    memory_volume_1 = os.path.join('/dev',resList[0],resList[3])
	    memory_volume_2 = os.path.join('/dev',resList[0],resList[5])
            print 'memory_volume_1: '
            print memory_volume_1
            print 'memory_volume_2: '
            print memory_volume_2
            memory_volume_list.append(memory_volume_1)
            memory_volume_list.append(memory_volume_2)
        return memory_volume_list

    def _filterSnappableDiskDevices(self,diskDeviceXmlElements):
        return filter(lambda(x): not(x.getAttribute('device')) or
                      x.getAttribute('device') in ['disk', 'lun'],
                      diskDeviceXmlElements)


    def rcd_get_diskName(self, vmName, vmID):
        """
        """
        
        cmd = 'cat /var/run/libvirt/qemu/%s.xml' % vmName
        srcDomXML = self.rcd_call_backendCmd(cmd, vmID)

        parsedSrcDomXML = _domParseStr(srcDomXML)
       
        domainXmlElements = parsedSrcDomXML.getElementsByTagName('domain')[0]
  
        #domainXmlElements = parsedSrcDomXML.childNodes[0].getElementsByTagName('domain')[0]

        allDiskDeviceXmlElements = domainXmlElements.getElementsByTagName('devices')[0].getElementsByTagName('disk')

        snappableDiskDeviceXmlElements = \
            self._filterSnappableDiskDevices(allDiskDeviceXmlElements)
 
        disk = {}
        for snappableDiskDeviceXmlElement in snappableDiskDeviceXmlElements:
            diskType = snappableDiskDeviceXmlElement.getAttribute('type')
            if diskType not in ['file', 'block']:
                continue
            devname = snappableDiskDeviceXmlElement.getElementsByTagName('target')[0].\
                        getAttribute('dev')
            source = snappableDiskDeviceXmlElement.getElementsByTagName('source')[0].\
                        getAttribute('dev')
            
            imageID = os.path.basename(os.path.dirname(source))
            disk[imageID] = devname

        return disk
            
        
    def rcd_get_actVolPath(self, volumeID, actVolID):
        """
        """
        cmd = "select * from images where image_guid='%s'" % volumeID 
        imageID = self.rcd_call_dbCmd(cmd).dictresult()[0].get('image_group_id')

        cmd = "select * from image_storage_domain_map"
        cmd += " where image_id='%s'" % volumeID
        domainID = self.rcd_call_dbCmd(cmd).dictresult()[0].get('storage_domain_id')

        cmd = "select storage_pool_id from storage_pool_iso_map "
        cmd += "where storage_id='%s'" % domainID
        poolID = self.rcd_call_dbCmd(cmd).dictresult()[0].get('storage_pool_id')

        actVolPath = ('/rhev/data-center/%s/%s/images/%s/%s')% ( poolID, domainID,
                                                                 imageID, actVolID)
        return actVolPath

    
    @calc_time_wrap
    def rcd_deleteSnap_fromDb(self,snapID):
        """
        """
        # delete snap from DB
        volumeID_list = self.rcd_get_volumeIDList(snapID)
        for volumeID in volumeID_list:
            print "==>"*20
            parentID = self.rcd_get_parentID(volumeID)
            cmd1 = "delete from image_storage_domain_map "
            cmd1 +="a where a.image_id='%s'" % volumeID
            cmd2 = "delete from disk_image_dynamic "
            cmd2 +="a where a.image_id='%s'"%volumeID
            cmd3 = "update images set parentid='%s' " % parentID
            cmd3 +="where parentid='%s'" % volumeID
            cmdList = [cmd1, cmd2, cmd3 ]
            for cmd in cmdList:
                print cmd
                self.rcd_call_dbCmd(cmd)

        cmd = "delete from snapshots "
        cmd +="a where a.snapshot_id='%s'"%snapID
        print cmd
        self.rcd_call_dbCmd(cmd)


    def rcd_get_volumeIDList(self, snapID):
        """
        """
        conn = self.conn
        cmd =  "select * from images where vm_snapshot_id='%s'" % snapID
        res = self.rcd_call_dbCmd(cmd).dictresult()
        volumeID_list = []
        for d in res:
            volumeID = d.get('image_guid')
            volumeID_list.append(volumeID)

        return volumeID_list

    def rcd_get_parentID(self, volumeID):
        """
        """
        cmd = "select * from images where image_guid='%s'" % volumeID
        parentID = self.rcd_call_dbCmd(cmd).dictresult()[0].get('parentid')
        return parentID

    def rcd_get_actVolID(self, volumeID):
        return self.rcd_get_parentID(volumeID)

    def rcd_call_dbCmd(self, cmd):
        """
        """
        conn = self.conn
        cmd = cmd
        print cmd
        return conn.query(cmd)

    def rcd_get_hostInfo(self, vmID):
        """
        """
        hostIP = ''
        for vmObj in self.api.vms.list():
            if vmObj.get_id() == vmID:
                hostIP = self.api.hosts.get(id=vmObj.host.id).address
        if hostIP:
            return ('root', hostIP, config.OVIRT_PASSWORD)
        
        return None
    

    def rcd_call_backendCmd(self,cmd,vmID):
        """ 
        """ 
        user,ip,password = self.rcd_get_hostInfo(vmID)
        print user,
        print ip
        print password
        ret = ''
        print 'call cmd: %s' % cmd
        print 'test=========='

        ssh = pexpect.spawn('ssh %s@%s "%s"'%(user,ip,cmd), timeout=330)
        try:
            expect = ssh.expect(['password', 'continue connecting (yes/no)?', ''])
            if expect == 0:
                print 'ssh expect ---> %d'% expect
                ssh.sendline(password)
            elif expect == 1:
                print 'ssh expect ---> %d'% expect
                ssh.sendline('yes')
                try:
                    expect = ssh.expect(['password'])
                    if expect == 0:
                        ssh.sendline(password)
                    else:
                        print ' [e] expect error.'
                except  pexpect.EOF:
                    ssh.close()
            elif expect == 2:
                print 'ssh expect ---> %d'% expect
                ssh.sendline('\n')
            else:
                print 'ssh expect ---> %d'% expect
                print 'ssh finished...'
        except pexpect.EOF:
            print '====?2'
            ssh.close()
        else:
            ret = ssh.read()
            ssh.expect(pexpect.EOF)
            ssh.close()
        print "test"
        return ret

if __name__ == '__main__':
    from pprint import pprint
    import sys
    if len(sys.argv) > 2:
        vmName = sys.argv[1]
        snapName = sys.argv[2]
    else:
        raise "Error"
    api = get_api()
    rcd = RedhatCmccDelete(api)

    #begin_time = Utils().rpc_get_current_time()

    vmObj = rcd.rcd_get_vmObj(vmName)
    vmID = vmObj.get_id()

    print 'vmName: ', vmName
    print 'vmID: ',vmID
    print 'snapName: ', snapName

    for disk in vmObj.get_disks().list():
        print disk.get_id()

    
    snapID = rcd.rcd_get_snapID(vmName, snapName)
    cmd = "select * from images where vm_snapshot_id='%s'" % snapID 
     
    res = rcd.rcd_call_dbCmd(cmd).dictresult()
    print res
    print '==>'*20
    print 'all volumeID: ',
    for r in res:
        pprint(r)
        print 
    print '==>'*20

    rcd.rcd_delete_snap(vmName, snapName)
    
