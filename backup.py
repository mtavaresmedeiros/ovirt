#!/usr/bin/python

from threading import Thread
from modules import *
from ovirtsdk.xml import params
from ovirtsdk.api import API
import sys

ENGINE_SERVER = "https://ovirt.com"
ENGINE_USER = "admin@internal"
ENGINE_PASS = "password"
ENGINE_CERT = "/etc/pki/ovirt-engine/ca.pem"

api = Connect(ENGINE_SERVER, ENGINE_USER, ENGINE_PASS, ENGINE_CERT)
if not api:
    Disconnect

vms = api.vms.list()

listas = [[], [], []]
indice = 0
for vm in vms:
    listas[indice].append(vm)
    indice += 1
    if indice == len(listas):
        indice = 0
try:
    t1 = Thread(target=backup, args=(listas[0],))
    t1.start()
    t2 = Thread(target=backup, args=(listas[1],))
    t2.start()
    t3 = Thread(target=backup, args=(listas[2],))
    t3.start()
    t1.join()
    t2.join()
    t3.join()
except:
    print "Error: unable to start thread"

if not MainExportDomain(EXPORT_NAME, SLEEP, TIME_LIMIT, DC_NAME):
    Disconnect(1)

if not DetachExpoDomain(DC_NAME, EXPORT_NAME, SLEEP, TIME_LIMIT):
    Disconnect(1)
