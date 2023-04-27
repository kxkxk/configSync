import etcd3
import threading
import toml
import socket
import queue
import asyncio
import time

HOST = None
CHANGE_TAG = '_NEEDSYNC_'
SELFCONFPATH = 'D:\PROJECTS\configSync\config.toml'
PUBLICREVERSION = None
CONFIGTABLE = {}
WORKQUEUE = None

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

class Config(object):
    publicDict = {}
    privateDict = {}
    src = ""
    reversions = {}
    name = ""
    lock = threading.Lock()
    
    def __init__(self, name, src):
        self.name = name
        self.src = src
        self.reversions['public'] = 0
    
    def set_public_reversion(self, reversion):
        self.reversions['public'] = reversion

    def get_public_reversion(self):
        return self.reversions['public']
    
    def should_update(self, key, reversion):
        print(self.reversions[key])
        return self.reversions[key] < reversion
    
    def set_private_reversion(self, key, reversion):
        self.reversions[key] = reversion

    def set_dict(self, configDict: dict):
        with self.lock:
            for section, values in configDict.items():
                if section == 'basic_settings':
                    continue
                for key, val in values.items():
                    if val == 'PUBLIC':
                        self.publicDict[section + '/' + key] = 1
                    if val == 'PRIVATE':
                        self.privateDict[section + '/' + key] = 1
                        self.reversions[section + '/' + key] = 0

    def __str__(self):
        return f"Name: {self.name}\n" \
               f"Source: {self.src}\n" \
               f"Public Dictionary: {self.publicDict}\n" \
               f"Private Dictionary: {self.privateDict}\n" \
               f"Reversions: {self.reversions}\n"
    


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
def load_config(path):
    with open(path, 'r', encoding='UTF-8') as f:
        config_dict = toml.load(f)
    return config_dict

# 将需要改变的值写入文件
def write(change_dict: dict):
    return

def check_config(confgDict: dict) -> bool:
    # TODO:实现格式检查
    return True

def get_host():
    hostname = socket.gethostname()
    return socket.gethostbyname(hostname)

async def write_file(path, config: Config):
    try:
        async with asyncio.Lock():
        # TODO:文件处理
            with open(path, 'a') as f:
                # f.write(value + '\n')
                pass
    except FileNotFoundError:
        print(f'Error: File {path} not found')

async def modify_worker(config: Config):
    while True:
        try:
            # 从队列中取出需要修改的值
            value = WORKQUEUE.get(timeout=1)
        except queue.Empty:
            # 如果队列为空，等待一段时间再尝试取值
            time.sleep(0.1)
            continue

        # if value is None:
        #     # 如果队列中取出 None，表示队列已经被清空，退出循环
        #     break
        await write_file(path=config.name, config=config)
        # 处理完一个值后，向队列发送信号
        WORKQUEUE.task_done()