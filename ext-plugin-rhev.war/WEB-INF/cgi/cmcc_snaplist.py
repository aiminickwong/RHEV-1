import optparse
import ovirtsdk.api
import config

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
 
class RedhatCmccSnapMap(object):
    """
    """
    def __init__(self, api):
        self.api = api
    
    def rcs_get_snapMap(self, vmName):
        """
        """
        snapMap = {}
        vmList = self.rcs_get_targetVmList(vmName)

        for vm in vmList:
            snapNameDict = self.rcs_get_snapNameDict(vm)
            snapMap.update(snapNameDict)

        return snapMap

    def rcs_get_snapNameDict(self, vmName):
        """
        """
        snapNameDict = {}
        vmObj = self.rcs_get_vmObj(vmName)

        for snapObj in vmObj.get_snapshots().list():
            if snapObj.get_description() == 'Active VM': 
                continue
            snapName = snapObj.get_description()
            k = snapName
            v = dict(snapObj=snapObj, 
	             snapName=snapName, 
                     vmObj=vmObj,
                     vmName=vmName)
            snapNameDict[k] = v

        return snapNameDict
            
    def rcs_get_vmObj(self, vmName):
        """
        """ 
        for vm in self.api.vms.list():
            if vm.get_name() == vmName: 
                return vm
        return None
        

    def rcs_get_targetVmList(self,vmName):
        """
        """
        allVmObjList = self.api.vms.list()
        allVmNameList = [ vo.get_name() for vo in allVmObjList ]
        targetVmList = self.rcs_filter_vm(allVmNameList, vmName)
        #print sorted(targetVmList)
        return sorted(targetVmList)
 
    def rcs_filter_vm(self, vmNameList, vmName):
        """
        """
        flag = PREFIX
        targetVmList = []
        if vmName.startswith(PREFIX):
            l = vmName.split(PREFIX)
            #vmName = l[1]
        else:
            for vn in vmNameList: 
                #r = vn.rsplit(flag, 1)
                #if r[0]==vmName and len(r[-1])==14 :
                #    targetVmList.append(vn)
                if vn.startswith(PREFIX+vmName+PREFIX):
                     targetVmList.append(vn)
        targetVmList.append(vmName)

        return targetVmList

if __name__ == '__main__':
    import time
    from pprint import pprint
    vmName = 'jfyang'
    snapName = 'snap2'
    api = get_api()
 
    startTime = Utils().rpc_get_current_time()
    rcsm = RedhatCmccSnapMap(api)
    snapMap = rcsm.rcs_get_snapMap(vmName)
    print sorted(snapMap)
    snapInfo = snapMap.get(snapName)
    print snapInfo
    vmObj = snapInfo.get('vmObj')
    print vmObj.get_id()
    endTime = Utils().rpc_get_current_time()
    usedTime = str(int(endTime) - int(startTime))
    print 'used time: ', 
    print usedTime


