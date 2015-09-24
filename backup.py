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

SNAPSHOT_NAME = 'BKP for export'  # Description the VM snapshot
cluster_name = 'local_host-Local'  # Cluster Name
EXPORT_NAME = 'backup'  # Export Name
DC_NAME = 'local_host-Local'  # Domain Name

# Attch Export Domain
try:
    if api.storagedomains.get(EXPORT_NAME):
        api.datacenters.get(DC_NAME).storagedomains.add(api.storagedomains.get(EXPORT_NAME))
        print 'Export Domain was attached successfully'

except Exception as e:
    print 'Failed to add export domain:\n%s' % str(e)
for vm in api.vms.list():
    VM_NAME = vm.name  # VM Name
    vm_bkp = VM_NAME + '_Backup'  # Last Name for VM Backup
    # Create snapshot
    try:
        print 'Creating a Snapshot ' + VM_NAME
        api.vms.get(VM_NAME).snapshots.add(params.Snapshot(description=SNAPSHOT_NAME, vm=api.vms.get(VM_NAME)))
        while api.vms.get(VM_NAME).status.state == 'image_locked':
            time.sleep(1)
    except Exception as e:
        print 'Failed to Create a Snapshot:\n%s' % str(e)

    time.sleep(10)

    print 'Snapshot finished'

    # Clone the snapshot into a VM
    print 'Clone into VM started'
    snapshot = params.Snapshot(id=api.vms.get(VM_NAME).snapshots.list(description=SNAPSHOT_NAME)[0].id)
    snapshot_p = params.Snapshots(snapshot=[snapshot])
    api.vms.add(params.VM(name=vm_bkp, cluster=api.clusters.get(cluster_name), snapshots=snapshot_p))
    while api.vms.get(vm_bkp).status.state == 'image_locked':
        time.sleep(10)
    print "Cloning finished"
    # Delete Snapshot
    print "Delete Snapshot"
    vm.snapshots.list(description=SNAPSHOT_NAME)[0].delete()

    # Export VM
    try:
        print 'Export  ' + vm_bkp + ' Started'
        api.vms.get(vm_bkp).export(params.Action(exclusive=True, force=True, async=False, storage_domain=api.storagedomains.get(EXPORT_NAME)))
        while api.vms.get(vm_bkp).status.state != 'down':
            time.sleep(1)
    except Exception as e:
        print 'Failed to export VM:\n%s' % str(e)

    print 'VM was exported successfully'
    # Delete vm_bkp
    try:
        print 'Delete ' + vm_bkp + ' Started'
        api.vms.get(vm_bkp).delete()
        while api.vms.get(VM_NAME):
            time.sleep(5)
    except Exception as e:
        print 'Failed to remove VM:\n%s' % str(e)

    print 'VM was removed successfully'
    print "Backup VM Finished" + VM_NAME
# Detach Export Domain
print 'Set Status Maintanence Export Domain'
api.datacenters.get(DC_NAME).storagedomains.get(EXPORT_NAME).deactivate()
while api.datacenters.get(DC_NAME).storagedomains.get(EXPORT_NAME).status.state != 'maintenance':
    time.sleep(1)
print 'Detached Export Storage Domain'
api.datacenters.get(DC_NAME).storagedomains.get(EXPORT_NAME).delete()

Disconnect(0)
