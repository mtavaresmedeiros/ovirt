#!/usr/bin/python
import os
import sys
import time
from subprocess import check_output
from ovirtsdk.xml import params
from ovirtsdk.api import API
import psycopg2
import logging
from datetime import date

today = date.today()

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s: %(levelname)s %(message)s',
                    filename='/var/log/backup/backup%s.log' % today)

completas = logging.getLogger('Completa')
completas.addHandler(logging.FileHandler('/var/log/backup/completas.log'))
faltando = logging.getLogger('Faltando')
faltando.addHandler(logging.FileHandler('/var/log/backup/faltando.log'))

ENGINE_SERVER = "https://ovirt/ovirt-engine/api"
ENGINE_USER = "user"
ENGINE_PASS = "password"
ENGINE_CERT = "/etc/pki/ovirt-engine/ca.pem"

# Connect Ovirt
def Connect(url, username, password, ca_file):
    try:
        global api
        api = API(url=url,
                  username=username,
                  password=password,
                  ca_file=ca_file)
        return api
    except Exception as e:
        return False


# Disconnect Ovirt
def Disconnect(exitcode):
    api.disconnect()
    sys.exit(exitcode)

# Connect Ovirt
api = Connect(ENGINE_SERVER, ENGINE_USER, ENGINE_PASS, ENGINE_CERT)
if not api:
    Disconnect

    
#Variables Global
STORAGE_BKP_NAME = "nome do storage que sera utilizado para backup, do mesmo jeito que esta no storage domain"
cluster = "nome do cluster"
DC_NAME="nome do data center"
Storage_Bkp_ID = api.storagedomains.get(STORAGE_BKP_NAME).get_id()
Datacenter_ID = api.datacenters.get(DC_NAME).get_id()
Path_Storage_Bkp = "/rhev/data-center/%s/%s" % (Datacenter_ID, Storage_Bkp_ID)
networks = api.networks.list()
SS_N = "Backup the VM"
SLEEP = 10
TIME_LIMIT = 400
TIME_LIMIT_EX = 3600
fromaddr = 'email'
toaddrs  = 'email'
SERVEREMAIL= 'smtp.gmail.com:587'
username = 'user'
password = 'senha'
list_rm = ['lista de exclusao de vms']
retencao = {'vm name': 7} #numero informa a quantiade de dias que fica retido um backup" 
dias = ['Seg','Ter','Qua','Qui','Sex','Sab','Dom']
day = date.today()
number_day = date.today().day
number_month = date.today().month
today = dias[day.weekday()]

def demote(user_uid, user_gid):
    def result():
        os.setgid(user_gid)
        os.setuid(user_uid)
    return result


def Createlist(vms,list_rm):
    try:
        list_vms = []
        lista_vms= []
        for vm in vms:
             list_vms.append(vm.name) 
        conn = psycopg2.connect("host='' dbname='' user=")
        cur = conn.cursor()
        cur.execute("SELECT vm_name FROM storage_domains,image_storage_domain_map,images,vm_static,vm_device WHERE image_storage_domain_map.image_id = images.image_guid AND storage_domains.id = image_storage_domain_map.storage_domain_id AND vm_static.vm_guid = vm_device.vm_id AND images.image_group_id = vm_device.device_id AND vm_device.device = 'disk' AND storage_domain_id = '%s'" % Storage_Bkp_ID )
        out = cur.fetchall()
        for vm_b in out :
            list_rm.append(vm_b[0])
        lista_vms = list(set(list_vms) - set(list_rm))
        lista_vms.sort()
        return lista_vms 
    except Exception as e:
        logging.error ('Falha ao criar lista com as vms:\n%s' % str(e) , exec_info=True)
        return False



def CheckSnapStatus(SS_N, vm, SLEEP, TIME_LIMIT):
    try:
        logging.info('Verificando status do snapshot %s' % vm.name)
        SNAPSHOTS = vm.snapshots.list(description=SS_N)
        TIME_WAITING = 0
        if SNAPSHOTS:
            SNAPSHOTS = vm.snapshots.list(description=SS_N)
            if SNAPSHOTS[0].get_snapshot_status() == 'locked':
                logging.info('O snapshot esta em uso, por favor aguarde... : %s' % vm.name)
            while True:
                SNAPSHOTS = vm.snapshots.list(description=SS_N)
                if SNAPSHOTS:
                    if SNAPSHOTS[0].get_snapshot_status() == 'ok':
                        logging.info('O snapshot nao esta mas em uso : %s' % vm.name)
                        return True
                        break
                    if TIME_WAITING >= TIME_LIMIT:
                        logging.error('Nao foi possivel verifcar o snapshot da vm:%s apos %s ;' %
                             (vm.name, TIME_LIMIT), str(e) ,exc_info=True)
                        raise
                    time.sleep(SLEEP)
                    TIME_WAITING += SLEEP
                else:
                    logging.info('Nao existe snapshot para a vm : %s' % vm.name)
                    return True
                    break

        else:
            logging.info('Nao foi encontrado nenhum snapshot %s' % vm.name)

            return False

    except Exception as e:
        logging.error('Falha ao verificar o snapshot:%s \n%s' % (vm.name, str(e)), exc_info=True)
        return False


def DeleteSnapshot(SS_N, vm,):
    try:
        logging.info('Deletando o snapshot da vm: %s' % vm.name)
        SNAPSHOTS = vm.snapshots.list(description=SS_N)
        if SNAPSHOTS:
            vm.snapshots.list(description=SS_N)[0].delete()

        return True

    except Exception as e:
        logging.error('Falha ao deletar snapshot:\n%s' % str(e), exc_info=True)
        return False


def CheckDelSnap(SS_N, SLEEP, TIME_LIMIT_EX, vm):
    try:
        SNAPSHOTS = vm.snapshots.list(description=SS_N)
        if SNAPSHOTS:
            TIME_WAITING = 0
            while True:
                if vm.snapshots.list(description=SS_N):
                    time.sleep(SLEEP)
                else:
                    break
                if TIME_WAITING >= TIME_LIMIT_EX:
                    logging.error('ERROR: O snapshot nao foi deletando apos %s seconds.' %
                          TIME_LIMIT_EX, exc_info=True)
                    raise
                TIME_WAITING += SLEEP
            logging.info('Snapshot deletado com sucesso %s' % vm.name)

        return True

    except Exception as e:
        logging.error('Falha ao tentar deletar snapshot:\n%s: %s' % (str(e), vm.name), exc_info=True)
        return False


def CreateSnapshot(vm, SS_N, SLEEP, TIME_LIMIT):
    try:
        logging.info ('Criando Snapshot para a vm: %s' % vm.name)
        VM_P = api.vms.get(vm.name)
        VM_P.snapshots.add(params.Snapshot(description=SS_N, vm=VM_P))
        TIME_WAITING = 0
        while True:
            snapshots = vm.snapshots.list(description=SS_N)
            if snapshots[0].get_snapshot_status() == 'ok':
                break
            if TIME_WAITING >= TIME_LIMIT:
                logging.error ('Snapshot nao foi criado apos %s seconds : %s' % (TIME_LIMIT, vm.name))
                raise
            time.sleep(SLEEP)
            TIME_WAITING += SLEEP
        logging.info ('Snapshot criado com sucesso %s.' % vm.name)
        return True
    except Exception as e:
        logging.error ('Falha ao criar snapshot:\n%s' % str(e), exc_info=True)
        return False

def CheckVmBkp(vm, SLEEP, TIME_LIMIT, retencao, today, number_day, number_month, day):
    try:
        logging.info ('Verificando se ja existe um backup para essa a vm : %s' % vm.name)
        vm_bkp_name = vm.name + '-%s-%s-%s' % (today, number_day, number_month)
        return  vm_bkp_name
    except Exception as e:
        logging.error ('ERROR: Falha ao verificar se vm ja possui vm:\n%s' % str(e), exc_info=True)
        return False

def CreateVM(vm,cluster,vm_bkp_name):
    try:
        logging.info ('Criado vm : %s' % vm_bkp_name)
        memo = vm.get_memory()
        memo_policy = vm.get_memory_policy()
        cores = vm.cpu.topology.cores
        sockets = vm.cpu.topology.sockets
        cpu = params.CPU(topology=params.CpuTopology(cores=int(cores), sockets=int(sockets)))
        api.vms.add(params.VM(name=vm_bkp_name, memory=memo, memory_policy=memo_policy , cpu=cpu, cluster=api.clusters.get(cluster), template=api.templates.get('Blank'), type_="server"))
        vm_bkp = api.vms.get(vm_bkp_name)
        nics = api.vms.get(vm.name).nics.list()
        for nic in nics:
            profile_id = nic.get_vnic_profile().id
            interface = nic.get_interface()
            for network in networks:
                if network.vnicprofiles.get(network.name).id == profile_id:
                    vm_bkp.nics.add(params.NIC(name=nic.name, network=params.Network(name=network.name), interface=interface))
        logging.info ('Vm criada com sucesso : %s' % vm_bkp_name)
        return vm_bkp
    except Exception as e:
        logging.error ('ERROR: Falha ao criar vm :%s \n%s' % (vm.name, str(e)), exc_info=True)
        return False


def CheckIdBaseSnap(vm, SS_N, disk):
    try:
        logging.info ('Verificando o ID do disco da vm: %s' % vm.name)
        image_id = vm.snapshots.list(description=SS_N)[0].disks.get(disk.name).get_image_id()
        conn = psycopg2.connect("host='' dbname='' user=")
        cur = conn.cursor()
        cur.execute("SELECT storage_pool_id, storage_domain_id FROM storage_domains,image_storage_domain_map,images,vm_static,vm_device WHERE image_storage_domain_map.image_id = images.image_guid AND storage_domains.id = image_storage_domain_map.storage_domain_id AND vm_static.vm_guid = vm_device.vm_id AND images.image_group_id = vm_device.device_id AND vm_device.device = 'disk' AND vm_static.vm_name = '%s' AND image_id = '%s'" % (vm.name, image_id ))
        out = cur.fetchall()
        storage_pool_id = out[0][0]
        storage_domain_id = out[0][1]
        group_id = vm.snapshots.list(description=SS_N)[0].disks.get(disk.name).get_id()
        Path_Disk_ID = "/dev/%s/%s" %(storage_domain_id, image_id)
        check_output(["/sbin/lvchange", "-ay", Path_Disk_ID])
        logging.info ('ID do disco verificando com sucesso da vm: %s' % vm.name)
        return Path_Disk_ID
    except Exception as e:
        logging.error ('ERROR: Falha ao verificar ID do disco da vm :%s \n%s' % (vm.name, str(e)), exc_info=True)
        return False


def CreateDisk(disk, vm, vm_bkp, STORAGE_BKP_NAME, TIME_LIMIT, SLEEP):
    try:
        TIME_WAITING = 0
        logging.info ('Criado novo disco para a vm : %s' % vm_bkp.name)
        disk_size = disk.get_size()
        disk_format = disk.get_format()
        disk_interface = disk.get_interface()
        disk_name_bkp = disk.name
        Format_Disk = disk.get_format()
        boot = disk.get_bootable()
        if Format_Disk != 'raw' :
            Format_Disk = 'cow'
        if boot:
            boot = True
        else:
            boot = False
        vm_bkp.disks.add(params.Disk(storage_domains=params.StorageDomains(storage_domain=[api.storagedomains.get(STORAGE_BKP_NAME)]),
                                                           name=disk_name_bkp,
                                                           size=disk_size,
                                                           status=None,
                                                           interface=disk_interface,
                                                           format=Format_Disk,
                                                           sparse=True,
                                                           bootable=boot))
        while True:
            if vm_bkp.disks.get(disk_name_bkp).status.state == 'ok' :
                break
            if TIME_WAITING >= TIME_LIMIT:
                logging.error ('Disco nao foi criado apos %s seconds : %s' % (TIME_LIMIT, vm_bkp.name))
                raise
            time.sleep(SLEEP)
            TIME_WAITING += SLEEP
        return disk_name_bkp , Format_Disk
    except Exception as e:
        logging.error ('ERROR: Falha ao criar disco: \n%s' %  str(e), exc_info=True)
        return False

def CheckIdBkp(disk, vm, vm_bkp, Path_Storage_Bkp):
    try:
        logging.info ('Verificando ID dos disco de backup : %s' % vm_bkp.name)
        disk_name = disk.name
        disk_id = disk.get_id()
        image_id = disk.get_image_id()
        disk_bkp = vm_bkp.disks.get(disk_name)  
        disk_name_bkp = disk_bkp.name
        disk_bkp_id = disk_bkp.get_id()
        image_bkp_id = disk_bkp.get_image_id()
        if disk_name_bkp == disk_name:
            Path_Disk_BKP = Path_Storage_Bkp + "/images/%s/%s" % (disk_bkp_id, image_bkp_id)
        else:
            logging.error ('Discos com nomes diferentes entre as vms %s e %s' (vm.name, vm_bkp.name))
            return False
        return Path_Disk_BKP
    except Exception as e:
        logging.error ('ERROR: Falha ao verificar ID do disco de backup: \n%s' %  str(e), exc_info=True)
        return False


def CopyDisk(Path_Disk_ID, Path_Disk_BKP, vm, vm_bkp,SS_N,disk,Format_Disk):
    try:
        logging.info ('Copiando Disco da %s para %s' %(vm.name, vm_bkp.name))
        if Format_Disk == 'cow' :
            Format_Disk = 'qcow2'
        else:
            Format_Disk = 'raw'
        check_output(['qemu-img', 'convert', Path_Disk_ID, '-O', Format_Disk, Path_Disk_BKP], preexec_fn=demote(36, 36))
        logging.info ('Disco Copiado com sucesso da %s para %s' %(vm.name, vm_bkp.name))
        return True
    except Exception as e:
        logging.error ('Falha ao copiar disco :\n%s' % str(e), exc_info=True)
        return False



def DelVmReten(vm, TIME_LIMIT, SLEEP, retencao, day, dias):
    try:
        TIME_WAITING = 0
        try: 
            reten = retencao[vm.name]
        except:
            reten = 1
        ant = date.fromordinal(day.toordinal() - reten)
        day_ant = dias[ant.weekday()]
        number_day_ant = date.fromordinal(day.toordinal() - reten).day
        number_month_ant = date.fromordinal(day.toordinal() - reten).month
        VM_BKP_ANT = vm.name + '-%s-%s-%s' % (day_ant, number_day_ant, number_month_ant)
        logging.info('Verificando se VM %s existe' % VM_BKP_ANT)
        if api.vms.get(VM_BKP_ANT):
            api.vms.get(VM_BKP_ANT).delete()
            while True:
                if api.vms.get(VM_BKP_ANT):
                    time.sleep(SLEEP)
                else:
                    break
                if TIME_WAITING >= TIME_LIMIT:
                    logging.error('ERROR: VM nao deletada apos %s segundos' %
                          TIME_LIMIT, exc_info=True)
                    raise
                TIME_WAITING += SLEEP
            logging.info('%s removida com sucesso ' % VM_BKP_ANT)
        else:
            logging.info('%s nao existe ' % VM_BKP_ANT) 
        return True
    except Exception as e:
        logging.error('Falha ao remover \n%s' %  str(e), exc_info=True)
        return False


# Export Domain
def AttachDomain(STORAGE_BKP_NAME, DC_NAME):
    try:
        DC = api.datacenters.get(DC_NAME)
        logging.info('Attaching Export Domain...')
        if DC.storagedomains.get(STORAGE_BKP_NAME):
            dc_status = DC.storagedomains.get(STORAGE_BKP_NAME).status.state
            if dc_status == 'maintenance':
                if DC.storagedomains.get(STORAGE_BKP_NAME).activate():
                    logging.info('Export Domain was activated successfully')
            else:
                logging.info('Export Domain already atached. Skipping.')
        else:
            DC.storagedomains.add(api.storagedomains.get(STORAGE_BKP_NAME))
            logging.info('Export Domain successfully attached.')

        return True

    except Exception as e:
        logging.error('Failed to add export domain:\n%s' % str(e), exc_info=True)

        return False


# Detach Export Domain
def DetachDomain(DC_NAME, STORAGE_BKP_NAME, SLEEP, TIME_LIMIT):
    try:
        while True:
            logging.info('Detaching Export Domain')
            DC = api.datacenters.get()
            DC.storagedomains.get(STORAGE_BKP_NAME).delete()
            while True:
                sd_status = api.storagedomains.get(STORAGE_BKP_NAME).status.state
                if sd_status == 'unattached':
                    break
                if TIME_WAITING >= TIME_LIMIT:
                    logging.error('ERROR: VM not deleted after %s seconds' %
                          TIME_LIMIT, exc_info=True)
                    raise
                time.sleep(SLEEP)
                TIME_WAITING += SLEEP
            logging.info('Export domain successfully detached')

            return True

    except Exception as e:
        logging.error('Failed detached export :\n%s' % str(e), exc_info=True)

        return False


def MainDomain(STORAGE_BKP_NAME, SLEEP, TIME_LIMIT, DC_NAME):
    try:
        TIME_WAITING = 0
        logging.info('Set Status Maintanence Export Domain')
        DC = api.datacenters.get(DC_NAME)
        DC.storagedomains.get(STORAGE_BKP_NAME).deactivate()
        while True:
            SD = DC.storagedomains.get(STORAGE_BKP_NAME)
            sd_status = SD.status.state
            if sd_status == 'maintenance':
                break
            if TIME_WAITING >= TIME_LIMIT:
                logging.error('ERROR: Export Domain not maintenance after %s seconds' %
                      TIME_LIMIT, exc_info=True)
                raise
            time.sleep(SLEEP)
            TIME_WAITING += SLEEP
        logging.info('Applied successfully')

        return True

    except Exception as e:
        logging.error('Failed to set maintenance export:\n%s' % str(e), exc_info=True)

        return False


# Inicio de Backup
def Backup(vm):
    try :
        vm = api.vms.get(vm)
        if CheckSnapStatus(SS_N, vm, SLEEP, TIME_LIMIT):
            if DeleteSnapshot(SS_N, vm):
                if not CheckDelSnap(SS_N, SLEEP, TIME_LIMIT_EX, vm):
                    raise
        if not CreateSnapshot(vm, SS_N, SLEEP, TIME_LIMIT):
            raise
        vm_bkp_name = CheckVmBkp(vm, SLEEP, TIME_LIMIT, retencao, today, number_day, number_month, day)
        if not vm_bkp_name:
            raise        
        vm_bkp = CreateVM(vm, cluster, vm_bkp_name)
        disks = vm.disks.list()
        for disk in disks:
            Path_Disk_ID = CheckIdBaseSnap(vm, SS_N, disk)
            if not Path_Disk_ID:
                raise
            disk_name_bkp , Format_Disk = CreateDisk(disk, vm, vm_bkp, STORAGE_BKP_NAME, TIME_LIMIT, SLEEP)
            if not disk_name_bkp and Format_Disk:
                raise
            Path_Disk_BKP = CheckIdBkp(disk, vm, vm_bkp, Path_Storage_Bkp)
            if not Path_Disk_BKP:
                raise
            if not CopyDisk(Path_Disk_ID, Path_Disk_BKP, vm, vm_bkp,SS_N,disk,Format_Disk):
                raise
        if not DelVmReten(vm, TIME_LIMIT, SLEEP, retencao, day, dias):
            raise
        if CheckSnapStatus(SS_N, vm, SLEEP, TIME_LIMIT):    
            if DeleteSnapshot(SS_N, vm):
                if not CheckDelSnap(SS_N, SLEEP, TIME_LIMIT_EX, vm):
                    logging.error('Failed to delete snapshot:\n%s' % str(e), exc_info=True)

    except Exception as e:
        logging.error('Failed to backup VM:\n%s' % str(e), exc_info=True)
