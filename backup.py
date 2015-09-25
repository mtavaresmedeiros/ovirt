#!/usr/bin/python

from ovirtsdk.xml import params
from ovirtsdk.api import API
import sys
#from time import sleep, localtime
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

SS_N = 'BKP for export'  # Description the VM snapshot
cluster_name = 'local_host-Local'  # Cluster Name
EXPORT_NAME = 'backup'  # Export Name
DC_NAME = 'local_host-Local'  # Domain Name
dc = api.datacenters.get(DC_NAME)
sd = api.storagedomains.get(EXPORT_NAME)
# Attch Export Domain
try:
    print 'Attached Export Domain'
    if dc.storagedomains.get(EXPORT_NAME):
        print 'Exporta Domain Attach'
    else:
        dc.storagedomains.add(api.storagedomains.get(EXPORT_NAME))
        print 'Export Domain was attached successfully'

except Exception as e:
    print 'Failed to add export domain:\n%s' % str(e)
for vm in api.vms.list():
    VM_NAME = vm.name  # VM Name
    vm_n = api.vms.get(VM_NAME)
    vm_bkp = VM_NAME + '_Backup'  # Last Name for VM Backup
    snapshots = vm.snapshots.list(description=SS_N)
    TIME_LIMIT = 60
    SLEEP = 10
    # Delete Previos snapshots
    try:
        print 'Verificando Previus Snapshots'
        snapshots = vm.snapshots.list(description=SS_N)
        if snapshots: 
            print 'Delete Snapshot'
            vm.snapshots.list(description=SS_N)[0].delete()
            TIME_WAITING = 0
            while True:
                if vm.snapshots.list(description=SS_N):
                    break
                if TIME_WAITING >= TIME_LIMIT:
                    print 'ERROR: Snapshot not deleted after %s checks' % limit
                    sys.exit(1)
                time.sleep(SLEEP)
                TIME_WAITING += SLEEP
    except Exception as e:
        print 'Failed Delete a Snapshot:\n%s' % str(e)
    # Create snapshot
    try:
        print 'Creating a Snapshot ' + VM_NAME
        vm_n.snapshots.add(params.Snapshot(description=SS_N, vm=vm_n))
        TIME_WAITING = 0
        while True:
            snapshots = vm.snapshots.list(description=SS_N)
            if snapshots[0].get_snapshot_status() == 'ok':
                break
            if TIME_WAITING >= TIME_LIMIT:
                print 'ERROR: Snapshot not deleted after %s checks' % limit
                sys.exit(2)
            time.sleep(SLEEP)
            TIME_WAITING += SLEEP
    except Exception as e:
        print 'Failed to Create a Snapshot:\n%s' % str(e)

    print 'Snapshot finished'
    # Delete VM Export Domain
    if sd.vms.get(vm_bkp): 
        print 'Excluding VM from Export Domain'
        vmb = sd.vms.get(vm_bkp)
        vmb.delete()
        while sd.vms.get(vm_bkp):
            time.sleep(SLEEP)
    # Clone the snapshot into a VM
    print 'Clone into VM started'
    snapshot = params.Snapshot(id=vm_n.snapshots.list(description=SS_N)[0].id)
    snapshot_p = params.Snapshots(snapshot=[snapshot])
    cl = api.clusters.get(cluster_name)
    api.vms.add(params.VM(name=vm_bkp, cluster=cl, snapshots=snapshot_p))
    while api.vms.get(vm_bkp).status.state == 'image_locked':
        time.sleep(SLEEP)
    print "Cloning finished"
    # Delete Snapshot
    print "Delete Snapshot"
    vm.snapshots.list(description=SS_N)[0].delete()
    while vm.snapshots.list(description=SS_N):
                time.sleep(SLEEP)
    # Export VM
    try:
        sd = api.storagedomains.get(EXPORT_NAME)
        print 'Export  ' + vm_bkp + ' Started'
        api.vms.get(vm_bkp).export(params.Action(storage_domain=sd))
        while api.vms.get(vm_bkp).status.state != 'down':
            time.sleep(SLEEP)
    except Exception as e:
        print 'Failed to export VM:\n%s' % str(e)

    print 'VM was exported successfull'
    # Delete vm_bkp
    try:
        print 'Delete ' + vm_bkp + ' Started'
        api.vms.get(vm_bkp).delete()
        while api.vms.get(vm_bkp):
            time.sleep(SLEEP)
    except Exception as e:
        print 'Failed to remove VM:\n%s' % str(e)

    print 'VM was removed successfully'
    print "Backup VM Finished" + VM_NAME
# Detach Export Domain
print 'Set Status Maintanence Export Domain'
dc.storagedomains.get(EXPORT_NAME).deactivate()
while dc.storagedomains.get(EXPORT_NAME).status.state != 'maintenance':
    time.sleep(SLEEP)
print 'Detached Export Storage Domain'
api.datacenters.get(DC_NAME).storagedomains.get(EXPORT_NAME).delete()

Disconnect(0)

