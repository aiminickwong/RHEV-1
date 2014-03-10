
import optparse
import ovirtsdk.api
import config
import pg
import pexpect

from time import sleep
from ovirtsdk.xml import params
from ovirtsdk.infrastructure import errors
from ovirtsdk.infrastructure import contextmanager
from ovirtsdk.infrastructure.brokers import VMSnapshot
from ovirtsdk.infrastructure.brokers import VMSnapshots

from utils import Utils
from cmcc_restore import RedhatCmccRestore

class SnapshotHandler(object):

    def __init__(self):
        self.api = self.ssh_get_api()
        self.db = self.ssh_get_db()

    def ssh_get_api(self):
        """
	"""
        api = ovirtsdk.api.API( url=config.OVIRT_URL, 
                                insecure=True,
                                username=config.OVIRT_USERNAME+'@'+config.OVIRT_DOMAIN,
                                password=config.OVIRT_PASSWORD,
                                ca_file=config.OVIRT_CA_FILE,)
	return api

    def ssh_get_db(self):
        """
	"""
        conn = pg.connect( dbname=config.OVIRT_DB_NAME,
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
        return [ self.ssh_get_snapName(snapObj) for snapObj in snapObjList]

    def ssh_get_snapObj(self, vmName, snapName):
        """
        """
        vmObj = self.ssh_get_vmObj(vmName)
        for snapObj in self.ssh_get_snapObjList(vmObj):
            if self.ssh_get_snapName(snapObj) == snapName:
                return snapObj
        return None
         
    
    def ssh_create_snap(self, vmName, snapName):
        """
        """
        vmObj = self.ssh_get_vmObj(vmName)
        snapList = self.ssh_get_snapList(vmObj)
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
        rcr = RedhatCmccRestore()
        vmObj = self.ssh_get_vmObj(vmName)
        vmID = self.ssh_get_vmID(vmName)
        print 'vmname', vmName
        print vmObj.status.state
        if vmObj.status.state == 'up':
            vmObj.shutdown()
        print 'vmObj.status.state'
        (vmID_new,vmID,snapID) = rcr.rcr_create_vm_by_snap(vmName, snapName)
        print 'vmID_new: ',vmID_new
        print 'vmID: ', vmID
        print 'snapID: ', snapID
        rcr.rcr_swap_mic(vmID_new,vmID)
        rcr.rcr_swap_name(vmID_new,vmID)
        rcr.rcr_restore_with_memory_snap(vmID_new,snapID)
        print 'All task done!!!'

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
    vm_list = ssh.ssh_get_vmList()
    print '==>'*20
    print 'vm_list:'
    print vm_list
    vm_obj = ssh.ssh_get_vmObj(vm_name)
    print '==>'*20
    print 'vm_name:',vm_name
    print 'vm_obj:',vm_obj
    snap_list = ssh.ssh_get_snapList(vm_obj)
    print '==>'*20
    print 'snap list: '
    print snap_list
    snap_name = snapName
    #ssh.ssh_create_snap(vm_name, snap_name)
    #ssh.ssh_restore_snap(vm_name, snap_name)
     
    

  
    




