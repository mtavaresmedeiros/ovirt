#!/usr/bin/python

from ovirtsdk.xml import params
from ovirtsdk.api import API
import sys
import time

ENGINE_SERVER = "https://ovirt.hybdc.com"
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
dc = api.datacenters.get(DC_NAME)
# Attch Export Domain
try:
    if dc.storagedomains.get(EXPORT_NAME):
        print 'Exporta Domain Attach'
    else:
        dc.storagedomains.add(api.storagedomains.get(EXPORT_NAME))
        print 'Export Domain was attached successfully'

except Exception as e:
        print 'Failed to add export domain:\n%s' % str(e)

for vm in api.storagedomains.get(EXPORT_NAME).vms.list():
    VM_NAME = vm.name  # VM Name
    #  Delete VM
    if api.vms.get(VM_NAME):
        print 'Deleting previous version of VM'
        api.vms.get(VM_NAME).delete()
    # Import the VM
    try:
        sd = api.storagedomains.get(EXPORT_NAME)
        vm_i = sd.vms.get(VM_NAME)
        sd_i = api.storagedomains.get(STORAGE_NAME)
        cl = api.clusters.get(name=CLUSTER_NAME)
        vm_i.import_vm(params.Action(storage_domain=sd_i, cluster=cl))
        print 'Import the VM started'
        while api.vms.get(VM_NAME).status.state != 'down':
            time.sleep(1)
    except Exception as e:
        print 'Failed to import VM:\n%s' % str(e)
    print 'VM Imported Sucessfully'
    # Excluding VM from Export Domain
    print 'Excluding VM from Export Domain'
    sd = api.storagedomains.get(EXPORT_NAME)
    vmb = sd.vms.get(VM_NAME)
    vmb.delete()
    while sd.vms.get(VM_NAME):
        time.sleep(5)
time.sleep(5)
print 'Set Status Maintanence Export Domain'
dc.storagedomains.get(EXPORT_NAME).deactivate()
while dc.storagedomains.get(EXPORT_NAME).status.state != 'maintenance':
    time.sleep(1)
print 'Detached Export Storage Domain'
api.datacenters.get(DC_NAME).storagedomains.get(EXPORT_NAME).delete()
print "Import VMS Finished"
Disconnect(0)

