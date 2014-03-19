
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


class SnapshotHandler(object):

	def __init__(self, vmName, hostInfo):
		self.vmName = vmName
		self.api = self.getApi()
		self.db = self.getDb()
		self.vm = self.getVmByName(self.vmName)
		self.hostInfo = hostInfo

	def getApi(self):
		return ovirtsdk.api.API(
						url=config.OVIRT_URL, insecure=True,
						username=config.OVIRT_USERNAME+'@'+config.OVIRT_DOMAIN,
						password=config.OVIRT_PASSWORD,
						ca_file=config.OVIRT_CA_FILE)

	def getDb(self):
		return pg.connect(
				dbname=config.OVIRT_DB_NAME,
				host=config.OVIRT_DB_HOST,
				user=config.OVIRT_DB_USER,
				passwd=config.OVIRT_DB_PASSWORD)

	def getVmList(self):
		return self.api.vms.list()

	def getVmByName(self, vmName):
		for vm in self.getVmList():
			if vm.get_name() == vmName:
				return vm

	def getVmId(self, vm):
		return vm.get_id()

	def getSnapObjListByVmObj(self, vm):	
		return vm.get_snapshots().list()

	def getSnapNameList(self, vm):
		snl = []
		for snap in self.getSnapObjListByVmObj(vm):
			snapName = snap.get_description()
			print "snapName: %s" % snapName
			print "snapID: %s" % snap.get_id()
			print "snapVolID: %s" % self._imageTableHandler(snap.get_id())[0]
			if not snapName.startswith('Active VM'):
				snl.append(snapName)
		return snl

	def getSnapObjByName(self, snapName):
		for snap in self.vm.get_snapshots().list():
			if snap.get_description() == snapName:
				return snap

	def _callDbCmd(self, sqlcmd):
		return self.db.query(sqlcmd)

	def createSnapshot(self, snapshotName):
		snapshots = self._getSnapshots(self.vm, self.api)
		print snapshots
		if snapshotName in self.listSnapshot():
			print 'snapshotName alreay existed.'
			return 'Error:snapshotName alreay existed.'
		snapshotParams = self._getSnapshotParams(
											snapshotName)
		print snapshotParams
		print 'Adding snapshot %s' % snapshotName
		if not self.vm.status.state == 'up':
			print 'vm state: %s' % self.vm.status.state
			print 'creating snapshot...'
			snapshots.add(snapshot=snapshotParams)
		else:
			print 'vm state: %s' % self.vm.status.state
			print 'creating snapshot...'
			snapshots.add(snapshot=snapshotParams)
			newSnap = self.getSnapObjByName(snapshotName)
			while True:
				sleep(5)
				print '...',
				newSnap = self.getSnapObjByName(snapshotName)
				if not newSnap:
					continue
				print newSnap.get_snapshot_status()
				if newSnap.get_snapshot_status() == 'ok':
					print 'finished...'
					break
			if not self.vm.status.state == 'up': 
				self.vm.start()
		return 'Snapshot %s created.' % snapshotName

	def _getSnapshots(self, vm, api):
		return VMSnapshots(self.vm, api.id)

	def _getSnapshotParams(self, snapshotName):
		return params.Snapshot(
						description=snapshotName)
	
	def listSnapshot(self):
		return self.getSnapNameList(self.vm)

	def deleteSnapshot(self, snapshotName):
		vmID = self.vm.get_id()
		print 'vmID: %s' % vmID
		snapID = self.getSnapObjByName(snapshotName).get_id()
		print 'snapID: %s' % snapID
		volumeID, parentID, _ = self._imageTableHandler(snapID)
		imageID = self._getImageID(snapID)
		print 'imageID: %s' % imageID
		domainID = self._getDomainID(volumeID)
		print 'domainID: %s' % domainID
		poolID = self._getPoolID(domainID)
		print 'poolID: %s' % poolID
		actVolID = self._getActiveVolId()
		print 'actVolID: %s' % actVolID
		active_path = ('/rhev/data-center/%s/%s/images/%s/%s')%(
										poolID,domainID,imageID,actVolID)
		print 'volumeID: %s' % volumeID
		print 'parentID: %s' % parentID
		print 'Deleting snapshot %s' % snapshotName
		print self._deleteSnapFromBackend(active_path, vmID, volumeID)
		self._deleteSnapFromDb(snapID, volumeID, parentID)
		print '...ok'
		print 'finished...'
		

	def restoreSnapshot(self, vmName, snapshotName):
		vmID = self.vm.get_id()
		print 'vmID: %s' % vmID
		snapID = self.getSnapObjByName(snapshotName).get_id()
		print 'snapID: %s' % snapID
		volumeID = self._getVolId(snapID)
		print 'volumeID: %s' % volumeID 
		imageID = self._getImageID(snapID)
		print 'imageID: %s' % imageID
		domainID = self._getDomainID(volumeID)
		print 'domainID: %s' % domainID
		poolID = self._getPoolID(domainID)
		print 'poolID: %s' % poolID
		actVolID = self._getActiveVolId()
		print 'actVolID: %s' % actVolID
		active_path = ('/rhev/data-center/%s/%s/images/%s/%s')%(
										poolID,domainID,imageID,actVolID)

		print 'active_path: %s' % active_path
		#print 'Before restore: self.vm.status.state: %s' % self.vm.status.state 
		#if not self.vm.status.state == 'down':
	#		print 'vm stop...'
#			self.vm.stop()
#		while True:
#			vm = self.getVmByName(vmName)
#			if vm.status.state == 'down':
#				print vm.status.state
#				break
#			else:
#				sleep(3)
#				continue
#
#		snap_dst_path = self._restoreSnapFromBackend(
#											active_path, vmID, volumeID )
#
#		print 'restore snap from backend...'
#		sleep(10)
#		print 'vm start...'
#		vm = self.getVmByName(vmName)
#		vm.start()
#		while True:
#			vm = self.getVmByName(vmName)
#			if vm.status.state == 'up':
#				print vm.status.state
#				break
#			else:
#				sleep(3)
#				continue
#
#		print 'restore finished...'
#		print 'all over...'

	def _delSnapDstDir(self, snap_dst_path):
		cmd = 'rm -rf %s' % snap_dst_path
		ret = self._callBackendCmd(cmd)
		print ret
	
	def _getActiveVolId(self):
		actSnapID = ''
		for snap in self.vm.snapshots.list():
			if snap.get_description() == 'Active VM':
				actSnapID = snap.get_id()
		actVolID,_,_ = self._imageTableHandler(actSnapID)
		return actVolID
		 
	def _restoreSnapFromBackend(self, active_path, vmID, volumeID, ):
		cmd = 'python /usr/share/vdsm/volumeBackendHandler.py restore %s %s %s'%(
													active_path, vmID, volumeID)
		print 'command: %s' % cmd
		ret = self._callBackendCmd(cmd)
		snap_dst_path = ''
		for line in ret.splitlines():
			print line
			if line.strip().startswith('snap_dst_path'):
				_,snap_dst_path = line.split(':')
		return snap_dst_path

	def _deleteSnapFromBackend(self, active_path, vmID, volumeID):
		cmd = 'python /usr/share/vdsm/volumeBackendHandler.py delete %s %s %s'%(
											active_path,vmID, volumeID, )
		print 'cmd:%s' % cmd
		ret = self._callBackendCmd(cmd)
		return ret

	def _getHostInfo(self):
		return tuple(self.hostInfo.split('@'))

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
		sqlcmd = "select storage_pool_id	\
				  from storage_pool_iso_map	\
				  where storage_id='%s'" % domainID
		ret = self._callDbCmd(sqlcmd).dictresult()[0]
		return ret.get('storage_pool_id')

	def _getVmRunPid(self, vmName):
		cmd = 'cat /var/run/libvirt/qemu/%s.pid' % vmName
		ret = self._callBackendCmd(cmd)
		for line in ret.splitlines():
			if line.startswith(':'):
				continue
			return line

	def _killVmRunPid(self, vmPid):
		cmd = 'kill -9 %s' % vmPid
		ret = self._callBackendCmd(cmd)

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
	parser.add_option('-s', action='store', dest='snapshotName')
	(opts, args) = parser.parse_args()
	return (opts, args)

if __name__ == '__main__':
	print 'hello world!!!'
	opts, args = get_option()
	hostInfo = opts.hostInfo
	option = opts.option
	vmName = opts.vmName
	snapshotName = opts.snapshotName
	print hostInfo, option, vmName, snapshotName
	sh = SnapshotHandler(vmName, hostInfo)		

	print "vmID:", sh.vm.get_id()
	#try:
	if option == 'create':
		print sh.createSnapshot(snapshotName)
	elif option == 'start':
		print sh.vm.start()
	elif option == 'stop':
		print sh.vm.stop()
	elif option == 'list':
		print sh.listSnapshot()
	elif option == 'delete':
		sh.deleteSnapshot(snapshotName)
	elif option == 'restore':
		sh.restoreSnapshot(vmName, snapshotName)
	elif option == 'state':
		print sh.vm.status.state
	elif option == 'host':
		print sh._getHostInfo()
	elif option == 'db':
		cmd = ''
		print 'cmd: %s' % cmd
		ret = sh._callDbCmd(cmd)
		for i in ret.dictresult():
			print i

	#except AttributeError or NameError:
	#	print 'Attribute Error, Please check your Input...'
	#except Exception as ex:
	#	print 'Error: %s' % ex


