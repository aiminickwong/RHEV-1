#!/usr/bin/env python
#-* - coding:utf-8 -*-


import os
import pg
from subprocess import Popen, PIPE
import time
from xml.dom import minidom
import xml.dom
import datetime


class Utils():

	# Redhat cmcc project
	def call_command(self, command):
        	""" 
        	调用命令行，并返回执行命令后的返回值.

        	"""
        	p = Popen(command, shell=True, stdout=PIPE)
        	return p.stdout.read()

        def curl_base_cmd(self):
        	"""
        	"""
        	cmd = 'curl -X '

        def curl_base_method(self, method, api):
        	"""
        	"""
        	password = 'Redhat1!'
        	cmd =   "curl -X %s " % method
        	cmd += "-H 'Content-type: application/xml'  "
        	cmd += "-u admin@internal:%s " % password
        	cmd += "-k "
                if api.startswith('/'):
        	   cmd += "https://rhevm.example.com:443%s " % api
                else:
        	   cmd += "https://rhevm.example.com:443/api/%s " % api
        	return cmd

        def curl_get_method(self, api):
        	"""
        	"""
        	cmd = self.curl_base_method('GET', api)
		print 'cmd: ',
		print cmd
        	ret = self.call_command(cmd)
        	return ret

        def curl_post_method(self, api, xmlStr):
        	"""
        	"""
        	cmd = self.curl_base_method('POST', api)
		cmd += "-d '%s'" % xmlStr
		print 'cmd: ',
		print cmd
		ret = self.call_command(cmd)

        def curl_put_method(self, api, xmlStr):
        	"""
        	"""
        	cmd = self.curl_base_method('PUT', api)
		cmd += "-d '%s'" % xmlStr
		print 'cmd: ',
		print cmd
		ret = self.call_command(cmd)

        def test__curl_get_method(self):
        	self.curl_get_method('vms')


        # XML methods
        def xml_to_ioBuffer(self,xmlStr): 
        	"""
        	"""
		ioBuffer = minidom.parseString(xmlStr)
        	return ioBuffer

        def xml_to_xmlStr(self, ioBuffer):
        	"""
       
        	"""
		doc = minidom.parse(ioBuffer)
		xmlStr = doc.toxml()
		return xmlStr
	
	def xml_get_text(self, nodelist):
		"""

		"""
		text_list = []
		for node in nodelist:
			if node.nodeType == node.TEXT_NODE:
				text_list.append(node.data)
		return ''.join(text_list)
		

        def rpc_get_vmRunPid(self, vmName):
        	"""
        	"""
		file = '/var/run/libvirt/qemu/%s.pid' % vmName
		if not os.path.exists(file):
			return None
        	cmd = 'cat %s' % file
        	ret = self.call_command(cmd)
        	return ret

        def rpc_kill_vmRunPid(self, vmName):
        	"""
        	"""
		pid = self.rpc_get_vmRunPid(vmName)
		if not pid:
			print ' [x] pid:%s is not alive.' % vmName
			return 
		cmd = 'kill -9 %s' % pid
		self.call_command(cmd)

	def test__rpc_kill_vmRunPid(self):
		vmName = 'vm01'
		self.rpc_kill_vmRunPid(vmName)
	
	def rpc_get_current_time(self):
		"""
		"""
		return datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d%H%M%S')

        # pg options
        def pg_create_conn(self):
       		return pg.connect(
       				dbname=config.OVIRT_DB_NAME,
       				host=config.OVIRT_DB_HOST,
       				user=config.OVIRT_DB_USER,
       				passwd=config.OVIRT_DB_PASSWORD)

if __name__=='__main__':
	#Utils().test__curl_get_method()
	Utils().test__rpc_kill_vmRunPid()
