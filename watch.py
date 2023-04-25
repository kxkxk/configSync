
import etcd3
from util import EtcdClient

class EtcdWatcher(object):
    etcdClient = None
    def __init__(self, etcdHosts):
        self.etcdClient = EtcdClient(etcdHosts, 5)
    
    def create_watcher(self, key, prefix: bool):
        node = self.etcdClient.pick_up()
        if prefix:
            watcher, cancel = node.watch_prefix('config/' + str(key))
            return watcher, cancel
        watcher, cancel = node.watch('config/' + str(key))
        return watcher, cancel
    
    def changeNodes(self, etcdHosts):
        self.etcdClient.change_list(etcdHosts)

# 监听公共值的变化
def watch_public(event):
    print('Watch event: type={}, key={}, value={}'.format(event.type, event.key.decode(), event.value.decode()))
    return
    
# 监听私有值的变化
def watch_private(event):
    return