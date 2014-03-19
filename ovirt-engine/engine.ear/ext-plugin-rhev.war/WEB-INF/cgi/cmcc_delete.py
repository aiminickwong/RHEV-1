#!/usr/bin/env python
#-*- coding:utf-8 -*-
#########################################################################
#  #snapshotHandler -o delete -v vm -s snap2
#########################################################################

from utils import Utils
from xml.dom import minidom
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
        vmID = self.rcd_get_vmID(vmName)
        print 'vmID: ',
        print vmID
        snapID = self.rcd_get_snapID(vmName, snapName)
        print 'snapID: ',
        print snapID

        self.rcd_deleteSnap_fromBackend(vmID,snapID)
        self.rcd_deleteSnap_fromDb(snapID)

    @calc_time_wrap
    def rcd_get_vmID(self, vmName):
        """
        """
        for vmObj in self.api.vms.list():
           if vmObj.get_name() == vmName:
               return vmObj.get_id()
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
    def rcd_deleteSnap_fromBackend(self, vmID, snapID):
        """
        """

        memory_volume_list = self.rcd_get_memoryVolumeList(snapID)
        for memory_volume in memory_volume_list:
            cmd = 'lvremove %s' % memory_volume
            print cmd
            self.rcd_call_backendCmd(cmd)
        
        volumeID_list = self.rcd_get_volumeIDList(snapID)
        for volumeID in volumeID_list:
            print '==>'*20
            print 'volumeID: ',
            print volumeID
            actVolID = self.rcd_get_actVolID(volumeID)
            print 'actVolID: ',
            print actVolID
            actVolPath = self.rcd_get_actVolPath(volumeID, actVolID)
            print 'actVolPath: ',
            print actVolPath
            cmd = 'python /usr/share/vdsm/volumeBackendHandler.py delete '
            cmd += '%s %s %s'% (actVolPath, vmID, volumeID)
            print cmd
            #self.rcd_call_backendCmd(cmd)

    def rcd_get_memoryVolumeList(self, snapID):
        """
        """
        cmd = "select memory_volume from snapshots where snapshot_id='%s'"%snapID
        res = self.rcd_call_dbCmd(cmd).dictresult()[0].get('memory_volume')
        resList = res.split(',')
        print '==>'*20
        print 'memory_volume: '
	memory_volume_1 = os.path.join('/dev',resList[0],resList[3])
	memory_volume_2 = os.path.join('/dev',resList[0],resList[5])
        print 'memory_volume_1: '
        print memory_volume_1
        print 'memory_volume_2: '
        print memory_volume_2
        memory_volume_list = []
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
                #self.rcd_call_dbCmd(cmd)

        cmd = "delete from snapshots "
        cmd +="a where a.snapshot_id='%s'"%snapID
        print cmd
        #self.rcd_call_dbCmd(cmd)


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
        print 'volumeID:%s de parentID:%s ' % (volumeID, parentID)
        return parentID


    def rcd_get_actVolID(self, volumeID):
        conn = self.conn
        cmd =  "select * from images where parentid='%s'" % volumeID
        res  = self.rcd_call_dbCmd(cmd).dictresult()
        if len(res)>0:
            volumeID = res[0].get('image_guid')
            return self.rcd_get_actVolID(volumeID)

        return volumeID


    def rcd_call_dbCmd(self, cmd):
        """
        """
        conn = self.conn
        cmd = cmd
        return conn.query(cmd)

    def rcd_get_hostInfo(self):
        """
        """
        #return ('root',self.rhevhIp,config.OVIRT_PASSWORD)
        return ('root','10.66.216.105',config.OVIRT_PASSWORD)
    

    def rcd_call_backendCmd(self,cmd):
        """ 
        """ 
        user,ip,password = self.rcd_get_hostInfo()
        print user,
        print ip
        print password
        ret = ''
        print 'call cmd: %s' % cmd

        print 'test=========='
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

if __name__ == '__main__':
    from pprint import pprint

    begin_time = Utils().rpc_get_current_time()

    api = get_api()

    rcd = RedhatCmccDelete(api)
    vmName = 'disktest'
    snapName = 'snap2'
    
    #snapID = rcd.rcd_get_snapID(vmName, snapName)

    #cmd = "select * from images where vm_snapshot_id='%s'" % snapID 
    #res = rcd.rcd_call_dbCmd(cmd).dictresult()
    #print '==>'*20
    #print 'all volumeID: ',
    #for r in res:
    #    pprint(r)
    #    print 
    #print '==>'*20
     
    
    #rcd.rcd_delete_snap(vmName, snapName)
    cmd = 'ls /'
    print rcd.rcd_call_backendCmd(cmd)
    

    end_time = Utils().rpc_get_current_time()
    print 'used time: ',
    print int(end_time)-int(begin_time)
    
