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

from ovirtsdk.xml import params
from ovirtsdk.infrastructure import errors
from ovirtsdk.infrastructure import contextmanager
from ovirtsdk.infrastructure.brokers import VMNic
from ovirtsdk.infrastructure.brokers import VMNics

from lxml import etree
from ovirt_image_uploader.ovf.ovfenvelope import *
import pg
import pexpect


from utils import Utils
PREFIX = config.PREFIX_FOR_VMNAME

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

class RedhatCmccRestore2Link(object):
    
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
        vmID = self.rcd_get_vmID(vmName)
        print 'vmID: ',
        print vmID
        snapID = self.rcd_get_snapID(vmName, snapName)
        print 'snapID: ',
        print snapID

        self.rcd_deleteSnap_fromBackend(vmID,snapID)
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
 
    
    def rcrl_link_restore(self, vmName, snapName):
        """
        """
        print "finding SPM from DB..."
        vmObj = self.rcd_get_vmObj(vmName)
        print vmObj.cluster.id 
        cmd = "SELECT ip,host_name from vds where spm_status=2 and vds_group_id='%s'" % vmObj.cluster.id
        d1 = self.rcd_call_dbCmd(cmd).dictresult()[0]
        hostIP=d1.get('ip')
        if not hostIP:
            hostIP = d1.get('host_name')
            #SELECT ip,host_name from vds where spm_status=2 and vds_group_id='99408929-82cf-4dc7-a532-9d998063fa95';
        snapID = self.rcd_get_snapID(vmName, snapName)
        snapVolumeID_list = self.rcd_get_volumeIDList(snapID)
        activeID = self.rcd_get_snapID(vmName, 'Active VM')
        activeVolumeID_list = self.rcd_get_volumeIDList(activeID)
        print "vmID=",vmObj.id
        vmID = vmObj.id
        print "hostIP=",hostIP
        print '==>'*20
        print 'snapVolumeID_list=',snapVolumeID_list
        print 'activeVolumeID_list=',activeVolumeID_list
        print '==>'*20
        activeDisks = {}
        snapDisks = {}
        for activeVolumeID in activeVolumeID_list:
            cmd = "SELECT image_group_id from images where image_guid='%s'" % activeVolumeID
            image_group_id = self.rcd_call_dbCmd(cmd).dictresult()[0].get('image_group_id')
            cmd = "SELECT image_guid,parentid,vm_snapshot_id from images where image_group_id='%s'" % image_group_id
            diskVolumes = self.rcd_call_dbCmd(cmd).dictresult()
            activeDisks[image_group_id] = diskVolumes
            #print '==>'*20
            #print activeVolumeID
            #print '==>'*20
        for snapVolumeID in snapVolumeID_list:
            cmd = "SELECT image_group_id from images where image_guid='%s'" % snapVolumeID
            image_group_id = self.rcd_call_dbCmd(cmd).dictresult()[0].get('image_group_id')
            cmd = "SELECT image_guid,parentid,vm_snapshot_id from images where image_group_id='%s'" % image_group_id
            diskVolumes = self.rcd_call_dbCmd(cmd).dictresult()
            snapDisks[image_group_id] = diskVolumes
            #diskName = self.rcd_get_diskName(vmID, volumeID)
            #diskVols[diskName] = volumeID
            #print snapVolumeID
        print '==>' * 30
        print 'activeDisks len: ',
        print len(activeDisks.keys())
        print 'activeDisks keys: '
        print activeDisks.keys()

        print 'snapDisks len: ',
        print len(snapDisks.keys())
        print 'snapDisks keys: '
        print snapDisks.keys()
        print '==>' * 30

        flag = 0
        for actDsk in activeDisks.keys():
            if snapDisks.has_key(actDsk):
                print flag 
                flag = flag + 1
                print '=3=>'*20
                for snapVol in snapDisks[actDsk]:
                    if snapVol['vm_snapshot_id'] == snapID:
                        print '=4=>'*20
                        print snapVol
                        for activeVol in activeDisks[actDsk]:
                            if activeVol['vm_snapshot_id'] == activeID:
                                print '=5=>'*20
                                print activeVol
                                print '=6=>'*20
                                #call back end link clone this snap volume to active volume
                                cmd = "SELECT storage_domain_id from image_storage_domain_map"
                                cmd += " where image_id='%s'" % activeVol['image_guid']
                                storageDomainID = self.rcd_call_dbCmd(cmd).dictresult()[0].\
                                                      get('storage_domain_id')
                                cmd = "SELECT storage_pool_id from vds_groups where "
                                cmd += "vds_group_id='%s'" % vmObj.cluster.id
                                storagePoolID = self.rcd_call_dbCmd(cmd).dictresult()[0].get('storage_pool_id')
                                activePath="/rhev/data-center/%s/%s/images/%s/%s" % (storagePoolID,storageDomainID,actDsk,activeVol['image_guid'])
                                print activePath
                                snapPath="/rhev/data-center/%s/%s/images/%s/%s" % (storagePoolID,storageDomainID,actDsk,snapVol['image_guid'])
                                print snapPath

                                cmd = 'python /usr/share/vdsm/restoreVolLink.py %s %s 0' % (activePath,snapPath)
                                print cmd
                                print self.rcrl_call_backendCmd(cmd, hostIP)

                                #update active volume's parentid to this snap volume's image_guid
                                cmd = "update images set parentid='%s' where image_guid='%s'" % (snapVol['image_guid'],activeVol['image_guid'])
                                self.rcd_call_dbCmd(cmd)

        #update active snapshot's memory to this snap's memory

        cmd = "select memory_volume from snapshots where snapshot_id='%s'" % snapID
        ret = self.rcd_call_dbCmd(cmd).dictresult()[0]
        hiberVolHandle = ret.get('memory_volume')
        print "------------hiberVolHandle-----------:",hiberVolHandle
        if hiberVolHandle:
            cmd = "update snapshots set memory_volume='%s' where snapshot_type='ACTIVE' and vm_id='%s'"%(hiberVolHandle,vmID)
            self.rcd_call_dbCmd(cmd)


        cmd = "select vm_configuration from snapshots where snapshot_id='%s'" % snapID
        vm_configurationStr = self.rcd_call_dbCmd(cmd).dictresult()[0].get('vm_configuration')

        tree = etree.fromstring(vm_configurationStr)
        iterator = tree.findall('Content/Section')

        cmd = "delete from vm_interface "
        cmd +="a where a.vm_guid='%s'" % vmID
        self.rcd_call_dbCmd(cmd)

        cmd = "delete from vm_interface_statistics "
        cmd +="a where a.vm_id='%s'" % vmID
        self.rcd_call_dbCmd(cmd)

        cmd = "delete from vm_device "
        cmd +="a where a.type='interface' and a.vm_id='%s'" % vmID
        self.rcd_call_dbCmd(cmd)

        apiID = self.api.id
        vnics = VMNics(vmObj, apiID)

        for sec in iterator:
                for attr in sec.attrib:
                    if str(sec.attrib[attr]).endswith('VirtualHardwareSection_Type'):
                        #print "tag(%s) text(%s) attr(%s) class(%s)" % (sec.tag, sec.text,sec.attrib, sec)
                        itemElement = sec.findall('Item')
                        for item in itemElement:
                            #print "item tag(%s) item text(%s) item attr(%s) class(%s)" % (item.tag, item.text,item.attrib, item)
                            resource_type = None
                            mItem = {}
                            for elem in item:
                                #print "tag(%s) value(%s)" % (elem.tag, elem.text)
                                if str(elem.tag).endswith('ResourceType') and elem.text == '10':
                                    resource_type = elem.text
                                    mItem['resource_type']=elem.text
                                elif str(elem.tag).endswith('MACAddress'):
                                    mItem['MACAddress']=elem.text
                                elif str(elem.tag).endswith('InstanceId'):
                                    mItem['InstanceId']=elem.text
                                elif str(elem.tag).endswith('OtherResourceType'):
                                    mItem['OtherResourceType']=elem.text
                                elif str(elem.tag).endswith('ResourceSubType'):
                                    mItem['ResourceSubType']=elem.text
                                elif str(elem.tag).endswith('speed'):
                                    mItem['speed']=elem.text
                                elif str(elem.tag).endswith('Linked'):
                                    mItem['Linked']=elem.text
                                elif str(elem.tag).endswith('Caption'):
                                    mItem['Caption']=elem.text
                                elif str(elem.tag).endswith('Connection'):
                                    mItem['Connection']=elem.text
                                elif str(elem.tag).endswith('Name'):
                                    mItem['Name']=elem.text
                                elif str(elem.tag).endswith('Address'):
                                    mItem['Address']=elem.text
                                else:
                                    mItem[str(elem.tag)]=elem.text

                            if mItem.has_key('resource_type') and mItem['resource_type'] == '10':
                                print '=8=>'*20
                                #print etree.tostring(item)
                                print mItem
                                print '=9=>'*20
                                nicParams = params.NIC(name=mItem['Name'])
                                vnics.add( nic=nicParams )
                                vnic_profile_id=''
                                cmd = "select id from vnic_profiles_view where name='%s'" % mItem['Connection']
                                vnic_profile_id=self.rcd_call_dbCmd(cmd).dictresult()[0].get('id')

                                #cmd = "INSERT INTO vm_interface (id, vm_guid,mac_addr, name, speed, type, linked,vnic_profile_id) "
                                #cmd += "VALUES ('%s', '%s', '%s', '%s', %s, %s,%s,'%s')" % (mItem['InstanceId'],vmID,
                                #     mItem['MACAddress'],mItem['Name'],mItem['speed'],mItem['ResourceSubType'],mItem['Linked'],vnic_profile_id)
                                cmd = "update vm_interface set mac_addr='%s' where vm_guid='%s' and name='%s'" % (mItem['MACAddress'],vmID,mItem['Name'])
                                self.rcd_call_dbCmd(cmd)

                                cmd = "update vm_interface set vnic_profile_id='%s'  where vm_guid='%s' and name='%s'" % (vnic_profile_id,vmID,mItem['Name'])
                                self.rcd_call_dbCmd(cmd)

                                cmd = "update vm_interface set type=%s  where vm_guid='%s' and name='%s'" % (mItem['ResourceSubType'],vmID,mItem['Name'])
                                self.rcd_call_dbCmd(cmd)

                                cmd = "select id from vm_interface where vm_guid='%s' and name='%s'" % (vmID,mItem['Name'])
                                mItem['InstanceId'] = self.rcd_call_dbCmd(cmd).dictresult()[0].get('id')

                                #cmd = "INSERT INTO vm_interface_statistics (id, vm_id)"
                                #cmd +=" VALUES ('%s', '%s') " % (mItem['InstanceId'],vmID)
                                #self.rcd_call_dbCmd(cmd)

                                #if not mItem['Alias']:
                                #    mItem['Alias'] = ''
                                #cmd = "INSERT INTO vm_device (device_id, vm_id, type, device, boot_order,is_plugged,is_readonly,alias,address) " 
                                #cmd +="VALUES ('%s', '%s', 'interface', '%s', %s, %s,%s,'%s','{bus=0x00, domain=0x0000, type=pci, slot=0x07, function=0x0}')" % (
                                #        mItem['InstanceId'],vmID,
				#	mItem['Device'],mItem['BootOrder'],mItem['IsPlugged'],mItem['IsReadOnly'],mItem['Alias'])
                                #self.rcd_call_dbCmd(cmd)

                                if mItem['Alias']:
                                    cmd = "update vm_device set alias='%s'  where device_id='%s'" % (mItem['Alias'],mItem['InstanceId'])
                                    self.rcd_call_dbCmd(cmd)
                                cmd = "update vm_device set boot_order=%s  where device_id='%s'" % (mItem['BootOrder'],mItem['InstanceId'])
                                self.rcd_call_dbCmd(cmd)
                                cmd = "update vm_device set is_plugged=%s  where device_id='%s'" % (mItem['IsPlugged'],mItem['InstanceId'])
                                self.rcd_call_dbCmd(cmd)
                                cmd = "update vm_device set device='%s'  where device_id='%s'" % (mItem['Device'],mItem['InstanceId'])
                                self.rcd_call_dbCmd(cmd)

                                #{'OtherResourceType': 'rhevm', 'Caption': 'Ethernet adapter on rhevm', 'Address': None, }

                                #if mItem['Linked'] == 'ture':
                                #    mItem['Linked']=Ture
                                #else:
                                #    mItem['Linked']=False
                                #mac = params.MAC(address=mItem['MACAddress']) 
                                             #mac=mac,
                                             #linked=mItem['Linked'],


        #power on the vm
        vmObj.start()

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

    def rcrl_call_backendCmd(self,cmd,ip):
        """ 
        """
        user='root'
        password=config.OVIRT_PASSWORD
        ret = ''
        print 'call cmd: %s' % cmd
        print 'test=========='

        ssh = pexpect.spawn('ssh %s@%s "%s"'%(user,ip,cmd), timeout=330)
        try:
            expect = ssh.expect(['password', 'continue connecting (yes/no)?',''], timeout=330)
            if expect == 0:
                print 'ssh expect --> %d' % expect
                ssh.sendline(password)
            elif expect == 1:
                print 'ssh expect --> %d' % expect
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
                print 'ssh expect --> %d' % expect
                #ssh.sendline('\n')
            else:
                print 'ssh finished...%d' % expect

        except pexpect.EOF:
            print '====?2'
            ssh.close()
        except pexpect.TIMEOUT:
                print "TIMEOUT"
        else:
            ret = ssh.read()
            ssh.expect(pexpect.EOF)
            ssh.close()
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
    
