import etcd3
from util import *

def get_watch():
    etcd_client = EtcdClient(["10.50.7.24:2379"], 5).pick_up()
    watcher, cancel = etcd_client.watch("/my/prefix\\")
    return watcher, cancel, etcd_client


# Cancel the watch when done.

# def test1():
#     wat, can, etcdc = get_watch()

#     for event in wat:
#         print("Received event:", event)

wat, can, etcdc = get_watch()
for event in wat:
    if event.value.decode() == 'no':
        can()
    print("Received event:", event)
