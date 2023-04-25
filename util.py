import etcd3
import threading
import toml

class EtcdClient(object):
    def __init__(self, hosts, retrytimes):
        self.etcd = etcd3
        self.hosts = {}
        self.retrytimes = retrytimes - 1
        self.lock = threading.RLock()
        for host in hosts:
            self.hosts[host] = 1
    def add_endpoint(self, host:str):
        self.hosts[host] = 1
    def pick_up(self):
        self.lock.acquire()
        finClient = None
        # 找可用endpoint,不可用节点将被判定为死亡且在retrytimes后恢复
        for client, times in self.hosts.items():
            if times > 0:
                if finClient == None:
                    try:
                        finClient = self.etcd.client(client.split(':')[0], client.split(':')[1])
                        finClient.status()
                    except Exception:
                        finClient = None
                        self.hosts[client] = -self.retrytimes
                        continue
            else:
                self.hosts[client] += 1
        # 若存活节点全挂，则重试死亡节点
        if finClient == None:
            for client, times in self.hosts.items():
                if times < 0:
                    try:
                        finClient = self.etcd.client(client.split(':')[0], client.split(':')[1])
                        finClient.status()
                        self.hosts[client] = 1
                        break
                    except Exception:
                        finClient = None
                        continue
        self.lock.release()
        return finClient
    def remove_endpoint(self, host):
        self.hosts.pop(host)
    def get_weight(self, host):
        return self.hosts[host]
    def change_list(self, hosts: list):
        if len(hosts) == 0:
            return
        diff_self_hosts = set(hosts).difference(self.hosts.keys())
        diff_hosts_self = set(self.hosts.keys()).difference(hosts)
        if len(diff_hosts_self) > 0:
            for i in diff_hosts_self:
                self.remove_endpoint(i)
        if len(diff_self_hosts) == 0:
            print("not new")
            return
        self.hosts.clear()
        for i in hosts:
            self.add_endpoint(i)

def acquire_lock(key, hostname, etcd):
    lease = etcd.lease(5)
    fail = etcd.transaction(
        compare=[
            etcd.transactions.value(key) == 'lock',
        ],
        success=[],
        failure=[
            etcd.transactions.put(key, hostname, lease = lease),
        ]
    )
    return not fail[0]

def release_lock(key, etcd):
    etcd.delete(f'/locks/{key}')

# 读toml配置
def read_self_config(path):
    with open(path, 'r') as f:
        config_dict = toml.load(f)
    return config_dict


# 将需要改变的值写入文件
def write(change_dict: dict):
    return