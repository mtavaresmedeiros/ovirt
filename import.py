#!/usr/bin/python

from ovirtsdk.xml import params
from ovirtsdk.api import API
import sys
import time

ENGINE_SERVER = "https://ovirt.com"
ENGINE_USER = "admin@internal"
ENGINE_PASS = "senha@123"
ENGINE_CERT = "/etc/pki/ovirt-engine/ca.pem"


def Connect(url, username, password, ca_file):
    global api
    api = API(url=url,
              username=username,
              password=password,
              ca_file=ca_file)


def Disconnect(exitcode):
    api.disconnect()

Connect(ENGINE_SERVER, ENGINE_USER, ENGINE_PASS, ENGINE_CERT)
print "Connected to %s successfully!" % api.get_product_info().name
EXPORT_NAME = 'backup'  # Export Domain Name
CLUSTER_NAME = 'local_host-Local'  # Cluster Name
DC_NAME = 'local_host-Local'  # Data Center Name
STORAGE_NAME = 'local_host-Local'  # Import VM Storage Name
# Attch Export Domain
try:
    if api.datacenters.get(DC_NAME).storagedomains.add(api.storagedomains.get(EXPORT_NAME)):
        print 'Export Domain was attached successfully'

except Exception as e:
        print 'Failed to add export domain:\n%s' % str(e)

for vm in api.storagedomains.get(EXPORT_NAME).vms.list():
    VM_NAME = vm.name  # VM Name
    #  Delete VM
    if api.vms.get(VM_NAME):
        print 'Deleting previous version of VM'
        api.vms.get(VM_NAME).delete()
    try:
        api.storagedomains.get(EXPORT_NAME).vms.get(VM_NAME).import_vm(params.Action(storage_domain=api.storagedomains.get(STORAGE_NAME), cluster=api.clusters.get(name=CLUSTER_NAME)))
        # Import the VM
        print 'Import the VM started'
        while api.vms.get(VM_NAME).status.state != 'down':
            time.sleep(1)
    except Exception as e:
        print 'Failed to import VM:\n%s' % str(e)
    print 'VM Imported Sucessfully'
    # Excluding VM from Export Domain
    print 'Excluding VM from Export Domain'
    SD = api.storagedomains.get(EXPORT_NAME)
    VMB = SD.vms.get(VM_NAME)
    VMB.delete()
    while VM_NAME in [vm.name for vm in api.storagedomains.get(EXPORT_NAME).vms.list()]:
        time.sleep(1)
print 'Set Status Maintanence Export Domain'
api.datacenters.get(DC_NAME).storagedomains.get(EXPORT_NAME).deactivate()
while api.datacenters.get(DC_NAME).storagedomains.get(EXPORT_NAME).status.state != 'maintenance':
    time.sleep(1)
print 'Detached Export Storage Domain'
api.datacenters.get(DC_NAME).storagedomains.get(EXPORT_NAME).delete()
print "Import VMS Finished"
Disconnect(0)

