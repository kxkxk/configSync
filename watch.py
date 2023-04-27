
import etcd3
from util import *

HOST = get_host()
class EtcdWatcher(object):
    etcdClient = None
    watcherIds = {}
    def __init__(self, etcdHosts):
        self.etcdClient = EtcdClient(etcdHosts, 5)
    
    def create_watcher(self, key: str, callback, prefix: bool):
        node = self.etcdClient.pick_up()
        if prefix:
            watcher_id = node.add_watch_prefix_callback(key, callback=callback)
        else: 
            watcher_id = node.add_watch_callback(key, callback=callback)
        self.watcherIds[key] = watcher_id
    
    def get_client(self):
        return self.etcdClient

    def remove_watch(self, key):
        watchId = self.watcherIds.get(key)
        if watchId == None:
            return False
        node = self.etcdClient.pick_up()
        node.cancel_watch(watchId)

# 监听公共值的变化
def watch_public_callback(response):
    print(response.header.revision)
    for i in response.events:
        print(i)
    return
    
# 监听私有值的变化
def watch_private_callback(response):
    currversion = response.header.revision
    for i in response.events:
        pList: list = i.key.decode().split('/')
        print(len(CONFIGTABLE))
        configer: Config = CONFIGTABLE[pList[pList.index(HOST) + 1]]
        key = pList[-2] + '/' + pList[-1]
        if isinstance(i,etcd3.events.PutEvent) and configer.should_update(key, currversion):
            # 这得上锁
            with configer.lock:
                configer.privateDict[key] = i.value.decode()
                # 请求写入文件

                configer.set_private_reversion(key, currversion)
        print(configer)
    return 