#!/usr/bin/python
from modules02 import *
from ovirtsdk.xml import params
from ovirtsdk.api import API
import sys
import time
import threading
start_time = time.time()

api = Connect(ENGINE_SERVER, ENGINE_USER, ENGINE_PASS, ENGINE_CERT)
if not api:
    Disconnect
vms = api.vms.list()
if not AttachDomain(STORAGE_BKP_NAME, DC_NAME):
    Disconnect
class VmsBackup(object):
    def __init__(self):
        self.vms = Createlist(vms,list_rm)
        faltando.info('%s' % self.vms)
    def get_vm_to_backup(self):
        try:
            return self.vms[0]
            faltando.info('%s' % self.vms)
            faltando.info('-----------------------------------------------------------------------')
        except:
            return None

    def set_vm_as_backed_up(self, vm):
        self.vms.remove(vm)

    def run(self, Thread, vm):
        name=threading.current_thread().name
        Thread.activate(name)
 
        logging.info ('%s : Fazendo backup da vm %s' % (name, vm))
        logging.info ('Threads Ativas: %s' % str(Thread).strip())
        Backup(vm)
        logging.info ('Terminou backup da vm %s' % vm)
        completas.info ('%s' % vm)
        Thread.deactivate(name)


class Threads(object):

    def __init__(self):
        super(Threads, self).__init__()
        self.active=[]
        self.lock=threading.Lock()

    def activate(self, name):
        with self.lock:
            self.active.append(name)

    def deactivate(self, name):
        with self.lock:
            self.active.remove(name)

    def count(self):
        with self.lock:
            return len(self.active)

    def __str__(self):
        with self.lock:
            return str(self.active)

if __name__ == '__main__':
    Thread = Threads()
    backup = VmsBackup()
    jobs=[]

    while backup.get_vm_to_backup():
        if threading.activeCount() > 2:
            continue
        else:
            vm = backup.get_vm_to_backup()
            job = threading.Thread(target=backup.run, args=(Thread, vm))
            backup.set_vm_as_backed_up(vm)
            job.daemon = True
            job.start()
        time.sleep(1)
    while threading.activeCount() > 1:
        time.sleep(1)

end_time = time.time()
total_time = ((end_time - start_time) / 3600)
logging.info('backup completed successfully in : %.2f hours' % total_time)
msg = ('backup completed successfully in : %.2f hours' % total_time)
SendEmail(msg)
