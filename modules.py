#!/usr/bin/python

from config import *
from ovirtsdk.xml import params
from ovirtsdk.api import API
import sys
import time


#Connect Ovirt
def Connect(url, username, password, ca_file):
    try:
        global api
        api = API(url=url,
                  username=username,
                  password=password,
                  ca_file=ca_file)
        print 'Connected to %s successfully!' % api.get_product_info().name
        return api
    except Exception as e:
        print 'Failure connect to ovirt'
        return False


#Disconnect Ovirt
def Disconnect(exitcode):
    api.disconnect()
    sys.exit(exitcode)


# Export Domain
def AttachDomain(EXPORT_NAME, DC_NAME):
    try:
        DC = api.datacenters.get(DC_NAME)
        print 'Attaching Export Domain...'
        if DC.storagedomains.get(EXPORT_NAME):
            if DC.storagedomains.get(EXPORT_NAME).status.state == 'maintenance':
                if api.datacenters.get(DC_NAME).storagedomains.get(EXPORT_NAME).activate():
                    print 'Export Domain was activated successfully'
            else:
                print 'Export Domain already atached. Skipping.'
        else:
            DC.storagedomains.add(api.storagedomains.get(EXPORT_NAME))
            print 'Export Domain successfully attached.'
        
        return True

    except Exception as e:
        print 'Failed to add export domain:\n%s' % str(e)
        
        return False


# Checking snapshots status
def CheckSnapStatus(SS_N, vm, SLEEP, TIME_LIMIT):
    try:
        print 'Checking previous snapshots...'
        SNAPSHOTS = vm.snapshots.list(description=SS_N)
        TIME_WAITING = 0
        if SNAPSHOTS:
            SNAPSHOTS = vm.snapshots.list(description=SS_N)
            if SNAPSHOTS[0].get_snapshot_status() == 'locked':
                print 'Snapshot in use , waiting...'
            while True:
                SNAPSHOTS = vm.snapshots.list(description=SS_N)
                if SNAPSHOTS:
                    if SNAPSHOTS[0].get_snapshot_status() == 'ok':
                        print 'Snapshot released'
                        return True
                        break
                    if TIME_WAITING >= TIME_LIMIT:
                        print 'ERROR: Snapshot in use after %s seconds.' % TIME_LIMIT
                        raise 
                    time.sleep(SLEEP)
                    TIME_WAITING += SLEEP
                else:
                    print 'Snapshot released'
                    return True
                    break

        else:
            print 'No snapshot found'
            
            return False

    except Exception as e:
        print 'Failed to release snapshot:\n%s' % str(e)

        return False

#Deleting Snapshot
def DeleteSnapshot(SS_N, vm, SLEEP, TIME_LIMIT_EX):
    try:
        print 'Deleting Snapshot.'
        SNAPSHOTS = vm.snapshots.list(description=SS_N)
        if SNAPSHOTS:
            vm.snapshots.list(description=SS_N)[0].delete()
            TIME_WAITING = 0
            while True:
                if vm.snapshots.list(description=SS_N):
                    time.sleep(SLEEP)
                else:
                    break
                if TIME_WAITING >= TIME_LIMIT_EX:
                    print 'ERROR: Snapshot not deleted after %s seconds.' % TIME_LIMIT_EX
                    raise
                TIME_WAITING += SLEEP
            print 'Snapshot successfully deleted'

        return True

    except Exception as e:
        print 'Failed delete a snapshot:\n%s' % str(e)

        return False


# Create snapshot
def CreateSnapshot(vm, VM_N, SS_N, SLEEP, TIME_LIMIT):
    try:
        print 'Creating snapshot ' + VM_N
        VM_P = api.vms.get(VM_N)
        VM_P.snapshots.add(params.Snapshot(description=SS_N, vm=VM_P))
        TIME_WAITING = 0
        while True:
            snapshots = vm.snapshots.list(description=SS_N)
            if snapshots[0].get_snapshot_status() == 'ok':
                break
            if TIME_WAITING >= TIME_LIMIT:
                print 'ERROR: Snapshot not create snapshot after %s seconds' % TIME_LIMIT
                raise
            time.sleep(SLEEP)
            TIME_WAITING += SLEEP
        print 'Snapshot successffully created.'

        return True

    except Exception as e:
        print 'Failed to create a snapshot:\n%s' % str(e)

        return False

    
# Deleting previous backup VM
def DelVmBkp(VM_BKP, vm, SLEEP, TIME_LIMIT):
    try:
        TIME_WAITING = 0
        print 'Checking previous backup VM...'
        if api.vms.get(VM_BKP): 
            print 'Deleting ' + VM_BKP
            api.vms.get(VM_BKP).delete()
            while True:
                if api.vms.get(VM_BKP):
                    time.sleep(SLEEP)
                else:
                    break
                if TIME_WAITING >= TIME_LIMIT:
                    print 'ERROR: VM not deleted after %s seconds' % TIME_LIMIT
                    raise
                TIME_WAITING += SLEEP
            print 'VM successfully removed'

            return True

        else:
            print 'No backup found'
            return True

    except Exception as e:
        print 'Failed to remove VM:\n%s' % str(e)
        return False


# Clone the snapshot into a VM
def CloneSnapshot(VM_BKP,VM_N,CLUSTER_NAME, SS_N, SLEEP, TIME_LIMIT_EX):
    try:
        VM_P = api.vms.get(VM_N)
        SNAPSHOT = params.Snapshot(id=VM_P.snapshots.list(description=SS_N)[0].id)
        SNAPSHOT_P = params.Snapshots(snapshot=[SNAPSHOT])
        CL = api.clusters.get(CLUSTER_NAME)
        api.vms.add(params.VM(name=VM_BKP, cluster=CL, snapshots=SNAPSHOT_P))
        TIME_WAITING = 0
        print 'Cloning ' + VM_N + '...'
        while True:
            if api.vms.get(VM_BKP).status.state == 'down':
                break
            if TIME_WAITING >= TIME_LIMIT_EX:
                print 'ERROR: Snapshot not cloned after %s seconds' % TIME_LIMIT_EX
                raise
            time.sleep(SLEEP)
            TIME_WAITING += SLEEP
        print 'Clone successfully created'

        return True

    except Exception as e:
        print 'Failed to Clone a Snapshot:\n%s' % str(e)

        return False    


# Checking if the VM exists in the export domain
def CheckVmExport(VM_BKP, SLEEP, TIME_LIMIT, EXPORT_NAME):
    try:
        SD = api.storagedomains.get(EXPORT_NAME)
        TIME_WAITING = 0
        print 'Checking if the VM exists in the export domain'
        if SD.vms.get(VM_BKP):
            print 'Excluding VM from Export Domain'
            VMB = SD.vms.get(VM_BKP)
            VMB.delete()
            while True:
                if SD.vms.get(VM_BKP):
                   time.sleep(SLEEP)
                else:
                    break
                if TIME_WAITING >= TIME_LIMIT:
                    print 'ERROR: VM not deleted after %s seconds' % TIME_LIMIT
                    raise
                TIME_WAITING += SLEEP
            print 'VM deleted successfully'

            return True

        else:
            print 'No vm found in export domain'
            return True

    except Exception as e:
        print 'Failed Delete a VM the Export Domain:\n%s' % str(e)

        return False


# Export VM
def ExportVM(VM_BKP,EXPORT_NAME, SLEEP, TIME_LIMIT_EX):
    try:
        SD = api.storagedomains.get(EXPORT_NAME)
        TIME_WAITING = 0
        api.vms.get(VM_BKP).export(params.Action(storage_domain=SD))
        print 'Starting export the VM...'
        while True:
            if api.vms.get(VM_BKP).status.state == 'down':
                break
            if TIME_WAITING >= TIME_LIMIT_EX:
                print 'ERROR: VM not Exported after %s seconds' % TIME_LIMIT_EX
                raise
            time.sleep(SLEEP)
            TIME_WAITING += SLEEP
        print 'VM exported successfully'

        return True

    except Exception as e:
        print 'Failed to export VM:\n%s' % str(e)

        return False


# Set Maintenance Export Domain
def MainExportDomain(EXPORT_NAME, SLEEP, TIME_LIMIT, DC_NAME):
    TIME_WAITING = 0
    print 'Set Status Maintanence Export Domain'
    try:
        DC = api.datacenters.get(DC_NAME)
        DC.storagedomains.get(EXPORT_NAME).deactivate()
        while True:
            if DC.storagedomains.get(EXPORT_NAME).status.state == 'maintenance':
                break
            if TIME_WAITING >= TIME_LIMIT:
                print 'ERROR: Export Domain not maintenance after %s seconds' % TIME_LIMIT
                raise
            time.sleep(SLEEP)
            TIME_WAITING += SLEEP
        print 'Applied successfully'

        return True

    except Exception as e:
        print 'Failed to set maintenance export VM:\n%s' % str(e)

        return False


# Detach Export Domain
def DetachExpoDomain(DC_NAME,EXPORT_NAME, SLEEP, TIME_LIMIT):
    try:
        while True:
            print 'Detaching Export Domain'  
            api.datacenters.get(DC_NAME).storagedomains.get(EXPORT_NAME).delete()
            while True:
                if api.storagedomains.get(EXPORT_NAME).status.state == 'unattached':
                    break
                if TIME_WAITING >= TIME_LIMIT:
                    print 'ERROR: VM not deleted after %s seconds' % TIME_LIMIT
                    raise
                time.sleep(SLEEP)
                TIME_WAITING += SLEEP
            print 'Export domain successfully detached'

            return True 
            
    except Exception as e:
        print 'Failed detached export VM:\n%s' % str(e)

        return False
# Inicio de Backup
def backup(lists):
    if not AttachDomain(EXPORT_NAME, DC_NAME):
        Disconnect(1)

    for vm in lists:
        VM_N = vm.name
        VM_BKP = VM_N + '_Backup'  # Last Name for VM Backup  

        if CheckSnapStatus(SS_N, vm, SLEEP, TIME_LIMIT):
            if not DeleteSnapshot(SS_N, vm, SLEEP, TIME_LIMIT_EX):
                Disconnect(1)

        if not DelVmBkp(VM_BKP, vm, SLEEP, TIME_LIMIT):
            Disconnect(1)
    
        if not CreateSnapshot(vm, VM_N, SS_N, SLEEP, TIME_LIMIT):
            Disconnect(1)

        if not CloneSnapshot(VM_BKP, VM_N, CLUSTER_NAME, SS_N, SLEEP, TIME_LIMIT_EX):
            Disconnect(1)

        if CheckSnapStatus(SS_N, vm, SLEEP, TIME_LIMIT):
            if not DeleteSnapshot(SS_N, vm, SLEEP, TIME_LIMIT_EX):
                Disconnect(1)

        if not CheckVmExport(VM_BKP, SLEEP, TIME_LIMIT,EXPORT_NAME):
            Disconnect(1)

        if not ExportVM(VM_BKP,EXPORT_NAME, SLEEP, TIME_LIMIT_EX):
            Disconnect(1)

        if not DelVmBkp(VM_BKP, vm, SLEEP, TIME_LIMIT):
            Disconnect(1)
