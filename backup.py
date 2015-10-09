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
threads = listas

indice = 0
for vm in vms:
    listas[indice].append(vm)
    indice += 1
    if indice == len(listas):
        indice = 0

for thread_idx in range(len(threads)):
    threads[thread_idx] = Thread(target=Backup, args=(listas[thread_idx],))
    threads[thread_idx].start()

for thread_idx in range(len(threads)):
    threads[thread_idx].join()

if not MainExportDomain(EXPORT_NAME, SLEEP, TIME_LIMIT, DC_NAME):
    Disconnect(1)

if not DetachExpoDomain(DC_NAME, EXPORT_NAME, SLEEP, TIME_LIMIT):
    Disconnect(1)
