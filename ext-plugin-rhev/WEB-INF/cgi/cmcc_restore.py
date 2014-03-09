#!/usr/bin/env python
#-*- coding:utf-8 -*-
#########################################################################
#  #snapshotHandler -o restore -v vm -s snap2
#########################################################################


from utils import Utils
from xml.dom import minidom
import xml.dom
import optparse
import time
import os

vmName = 'vm01'
snapName = 'snap2'
option = 'restore'

class RedhatCmccRestore(object):

    def __init__(self):
        pass

    def rcr_get_snap(self,vmID,snapID):
        """
        根据snapName获取snapinfo.
        """
        api = 'vms/%s/snapshots/%s'% (vmID, snapID)
        snapInfo = Utils().curl_get_method(api)
        return snapInfo

    def rcr_get_vmInfo(self, vmID):
        """
        根据vmName获取vmInfo.
        """
        api = "vms/%s" % vmID
        vmInfo = Utils().curl_get_method(api)
        return vmInfo

    def rcr_get_snapID(self, vmID, snapName):
        """
        """
        api = 'vms/%s/snapshots' % vmID
        snapsInfo = Utils().curl_get_method(api)
        ioBuffer = Utils().xml_to_ioBuffer(snapsInfo)
        root = ioBuffer.documentElement
        snap_nodes = root.getElementsByTagName('snapshot')
        for snap_node in snap_nodes:
            snap_name_node = snap_node.getElementsByTagName('description')[0]
            snap_name = Utils().xml_get_text(snap_name_node.childNodes).strip()
            if snap_name == snapName:
                snapID = snap_node.getAttribute('id')
                return snapID
        return None
        

    def rcr_get_vmName(self, vmID):
        """
        """
        vmInfo = rcr.rcr_get_vmInfo(vmID)
        ioBuffer = Utils().xml_to_ioBuffer(vmInfo)
        root = ioBuffer.documentElement
        vm_name_node = root.getElementsByTagName('name')[0]
        vm_name = Utils().xml_get_text(vm_name_node.childNodes).strip()
        return vm_name
        
    def rcr_get_vmID(self, vmName):
        """
        """
        api = 'vms'
        vmInfo = Utils().curl_get_method(api)
        ioBuffer = Utils().xml_to_ioBuffer(vmInfo)
        root = ioBuffer.documentElement
        vm_nodes = root.getElementsByTagName('vm')
        for vm_node in vm_nodes: 
            vm_name_node = vm_node.getElementsByTagName('name')[0]
            vm_name = Utils().xml_get_text(vm_name_node.childNodes).strip()
            if vm_name == vmName:
                vmID = vm_node.getAttribute('id')
                return vmID
        return None
    
    def rcr_rename_vm(self, oldName, newName):
        """
        更新vmName.
        """
        pass

    def rcr_create_vm(self, xmlStr):
        """
        使用snap创建新的vm.
        """
        api = 'vms'
        Utils().curl_post_method(api, xmlStr)

    #"VM cmcc_20140307222151 creation has been completed."
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

    def rcr_create_vm_by_snap(self, vm_name,snap_name,sync=1):
        """
        使用snap创建新的vm.
        """
        vmID = self.rcr_get_vmID(vm_name)
        if not vmID:
            raise Exception #' [e] Can not find vmID by vmName:%s!' % vm_name
        snapID = self.rcr_get_snapID(vmID, snap_name)

        if not snapID:
            raise Exception #' [e] Can not find snapname[%s] on vmName[%s]!'% (
                  #  snap_name,vm_name)

        #create xmlStr
        vmInfo = rcr.rcr_get_vmInfo(vmID)
        ioBuffer = Utils().xml_to_ioBuffer(vmInfo)
        root = ioBuffer.documentElement

        dom1=xml.dom.getDOMImplementation()
        doc = dom1.createDocument(None,'vm',None)

        snapshots_node = doc.createElement('snapshots')
        snapshot_node = doc.createElement('snapshot')
        snapshot_node.setAttribute('id', snapID)
        snapshots_node.appendChild(snapshot_node)
        root.appendChild(snapshots_node)

        name = root.getElementsByTagName('name')[0]
        root.removeChild(name)

        name_node = doc.createElement('name')
        vm_name_new = '%s_%s'% (vmName,Utils().rpc_get_current_time())
        name_txt = doc.createTextNode(vm_name_new)
        name_node.appendChild(name_txt)
        root.appendChild(name_node)

        xmlStr = root.toxml()
        
        f=open('/tmp/aaa','w')
        f.write(xmlStr)
        f.close()

        self.rcr_create_vm(xmlStr)

        retry = 30
        vmID_new = None    
        if sync == 1:
            while 1:
                if not vmID_new:  
                    vmID_new = self.rcr_get_vmID(vm_name_new)
                    print 'vmID_new: ', vmID_new
                    time.sleep(10)
                    retry = retry - 1
                    if retry < 0:
			break
                    else:
                        continue
                self.rcr_wait_event("VM %s creation has been completed." % vm_name_new)
                vm_status = self.rcr_get_vm_status( vmID_new )
                print '==>'* 20
		print 'vm_status: ',
                print vm_status
                print '==>'* 20
                if vm_status !=  'down':
                    time.sleep(10)
                else:
                    break

            return vmID_new,vmID, snapID


    def rcr_get_vm_status(self, vmID):
        """
        """
        vmInfo = rcr.rcr_get_vmInfo(vmID)
        ioBuffer = Utils().xml_to_ioBuffer(vmInfo)
        root = ioBuffer.documentElement
        status_node = root.getElementsByTagName('status')[0]
        state_node = status_node.getElementsByTagName('state')[0]
        vm_status = Utils().xml_get_text(state_node.childNodes).strip()
        return vm_status

    def rcr_update_vmName(self,vmID, vmName):
        """
        """
        vmInfo = rcr.rcr_get_vmInfo(vmID)
        ioBuffer = Utils().xml_to_ioBuffer(vmInfo)
        root = ioBuffer.documentElement

        dom1=xml.dom.getDOMImplementation()
        doc = dom1.createDocument(None,'vm',None)

        name = root.getElementsByTagName('name')[0]
        root.removeChild(name)

        name_node = doc.createElement('name')
        name_txt = doc.createTextNode(vmName)
        name_node.appendChild(name_txt)
        root.appendChild(name_node)

        xmlStr = root.toxml()
        api = 'vms/%s' % vmID
        Utils().curl_put_method(api, xmlStr)

    def rcr_swap_mic(self, vmID_new, vmID):
        """
        """
        mac_map = self.rcr_get_nics_dict(vmID)
        self.rcr_set_nics_dict(vmID_new, mac_map)


    def rcr_set_nics_dict(self, vmID, mac_map):
        """
        """
        #nicInfo = rcr.rcr_get_nic(vmID)
        api = 'vms/%s/nics' % vmID
        nicInfo = Utils().curl_get_method(api)
        print 'nicInfo: ',
        print nicInfo
        nicBuffer = Utils().xml_to_ioBuffer(nicInfo)
        root = nicBuffer.documentElement
        nics = root.getElementsByTagName('nic')

        dom1=xml.dom.getDOMImplementation()
        doc = dom1.createDocument(None,'vm',None)

        for nic in nics:
            mac_node =nic.getElementsByTagName('mac')[0]    
            addr = mac_node.getAttribute('address')
            name_node = nic.getElementsByTagName('name')[0]
            name = Utils().xml_get_text(name_node.childNodes)
            #mac_map[name]= addr
            if mac_map.has_key(name):
                parent_node = mac_node.parentNode
                parent_node.removeChild(mac_node)
                node = doc.createElement('mac')    
                node.setAttribute('address', mac_map[name])
                parent_node.appendChild(node)
                id = nic.getAttribute('id')
                new_xml_str = nic.toxml()
                api = 'vms/%s/nics/%s' % (vmID, id)
                Utils().curl_put_method(api , new_xml_str)


    def rcr_get_nics_dict(self, vmID):
        """
        """
        api = 'vms/%s/nics' % vmID
        nicInfo = Utils().curl_get_method(api)
        
        nicBuffer = Utils().xml_to_ioBuffer(nicInfo)
        root = nicBuffer.documentElement
        nics = root.getElementsByTagName('nic')
        
        mac_map = {}
        for nic in nics:
            mac_node =nic.getElementsByTagName('mac')[0]    
            addr = mac_node.getAttribute('address')
            name_node = nic.getElementsByTagName('name')[0]
            name = Utils().xml_get_text(name_node.childNodes)
            mac_map[name]= addr
        return mac_map
    
    def rcr_swap_name(self,vmID_new,vmID):
        """
        """
        vm_name = self.rcr_get_vmName(vmID)
        vm_name_new = self.rcr_get_vmName(vmID_new)
        vmName_tmp = vm_name +"-"+ Utils().rpc_get_current_time()
        #print vm_name
        #print vm_name_new
        #print vmName_tmp
        self.rcr_update_vmName(vmID, vmName_tmp)
        self.rcr_update_vmName(vmID_new, vm_name)
        self.rcr_update_vmName(vmID, vm_name_new)

    def rcr_restore_with_memory_snap(self,vmID_new,snapID):
        """
        """
	snapID = snapID
        #get_snap_memery_path
        snap_src_path = self.rcr_get_snap_memery_path(vmID, snapID)
        snap_dst_path = self.rcr_get_active_path(vmID_new)
        src_dddd = os.path.join(snap_src_path,"dddd")
        src_eeee = os.path.join(snap_src_path,"eeee")
        dst_dddd = os.path.join(snap_dst_path,"dddd")
        dst_eeee = os.path.join(snap_dst_path,"eeee") 
        if os.path.exists(src_dddd) and os.path.exists(src_eeee):
            print "This snapshot has memory"
            #if os.path.exists(snap_dst_path):
                #removeSnapDstDir(active_volPath.split("/images")[0].split('data-center')[1]+active_volPath.split("images")[1])
            os.makedirs(snap_dst_path)
            os.symlink(src_dddd, dst_dddd)
            os.symlink(src_eeee, dst_eeee)
            print dst_dddd
         
	api = 'vms/%s/start' % vmID_new
        Utils().curl_post_method(api,"<action/>")

	
    def rcr_get_active_path(self, vmID):
        """
        """
        api = 'vms/%s/disks' % vmID
        disks = Utils().curl_get_method(api)
        diskBuffer = Utils().xml_to_ioBuffer(disks)
        root = diskBuffer.documentElement
        disk_node = root.getElementsByTagName('disk')[0]
        active_node = disk_node.getElementsByTagName('image_id')[0]
        active_vol = Utils().xml_get_text(active_node.childNodes)
        dcID = self.rcr_get_datacenter(vmID)
        images = self.rcr_get_images(vmID)
        active_path = os.path.join('/rhev_snap', dcID, images[0], active_vol)
        return active_path

    def rcr_get_snap_memery_path(self, vmID, snapID, ):
	"""
	"""
        dcID = self.rcr_get_datacenter(vmID)
        images = self.rcr_get_images(vmID)
        imageID = images[0]
        snapVolList = self.rcr_get_snapVolumes(snapID) 
        for imageID in images:
            path = os.path.join('/rhev_snap', dcID, imageID)
            if os.path.exists(path):
       	        for snapVol in snapVolList:
                    path1 = os.path.join(path, snapVol)
                    if os.path.exists(path1):
                        return path1
	
    def rcr_get_snapVolumes(self, snapID):
        """
        """
        api = 'vms/%s/snapshots/%s/disks' % (vmID,snapID)
        snap_disks = Utils().curl_get_method(api)
        snapdiskBuffer = Utils().xml_to_ioBuffer(snap_disks)
        root = snapdiskBuffer.documentElement
        snap_disk_nodes = root.getElementsByTagName('disk')
        snapVolList = []
        for snap_disk_node in snap_disk_nodes:
            snap_vol_node = snap_disk_node.getElementsByTagName('image_id')[0]
            snap_vol = Utils().xml_get_text(snap_vol_node.childNodes)
            snapVolList.append(snap_vol)

        return snapVolList

    def rcr_get_images(self, vmID):

        api = 'vms/%s/disks' % vmID
        disks = Utils().curl_get_method(api)
        diskBuffer = Utils().xml_to_ioBuffer(disks)
        root = diskBuffer.documentElement
        disk_nodes = root.getElementsByTagName('disk')
        images = []
        for disk_node in disk_nodes:
            image_id = disk_node.getAttribute('id')
            domain_nodes = disk_node.getElementsByTagName('storage_domains')[0]
            domain_node = domain_nodes.getElementsByTagName('storage_domain')[0]
            domain_id = domain_node.getAttribute('id')
            image = domain_id + '/' + image_id
            images.append(image)
        return images
    
    def rcr_get_datacenter(self, vmID):
	"""
	"""
	api = 'vms/%s' % vmID
	vmInfo = Utils().curl_get_method(api)
	vmBuffer = Utils().xml_to_ioBuffer(vmInfo)
	root = vmBuffer.documentElement
	cluster_node = root.getElementsByTagName('cluster')[0]
        api = cluster_node.getAttribute('href')
        #print api
        dcInfo = Utils().curl_get_method(api)
        dcBuffer = Utils().xml_to_ioBuffer(dcInfo)
        dc_root = dcBuffer.documentElement
        dc_node = dc_root.getElementsByTagName('data_center')[0]
        dcID = dc_node.getAttribute('id')
        return dcID


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
    #hostInfo = opts.hostInfo
    option = opts.option
    vmName = opts.vmName
    snapName = opts.snapName
    print option, vmName, snapName

    rcr = RedhatCmccRestore()

    Utils().rpc_kill_vmRunPid(vmName) 

    #vmID = rcr.rcr_get_vmID(vmName)
    #print 'vmID: ',vmID
    #snapID = rcr.rcr_get_snapID(vmID, snapName)
    #print 'snapID: ', snapID

    #rcr.rcr_get_snap_memery_path(vmID, snapID)

    #mac_map = rcr.rcr_get_nics_dict(vmID)
    #print mac_map
    #rcr.rcr_set_nics_dict(vmID, mac_map)
    
    (vmID_new,vmID,snapID) = rcr.rcr_create_vm_by_snap(vmName, snapName)
    rcr.rcr_swap_mic(vmID_new,vmID)
    rcr.rcr_swap_name(vmID_new,vmID)

    rcr.rcr_restore_with_memory_snap(vmID_new,snapID)
    print 'All task done!!!'
    #OK
    #rcr.rcr_rename_vm('vm01', 'tmp_vm')
