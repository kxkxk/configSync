import etcd3
import threading
import toml
import socket
import queue
import asyncio
import time
import enum
import configparser

HOST = None
CHANGE_TAG = '_NEEDSYNC_'
SELFCONFPATH = 'D:\PROJECTS\configSync\config.toml'
PUBLICREVERSION = None
CONFIGTABLE = {}
WORKQUEUE = queue.Queue()

class InputType(enum.Enum):
    PUBLIC = 1
    PRIVATE = 2
    SELFC = 3


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
    type = None
    lock = threading.Lock()
    
    def __init__(self, name, src):
        self.name = name
        self.src = src
        self.reversions['public'] = 0

    def set_type(self, ctype: InputType):
        self.type = ctype

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
                        self.publicDict[section + '/' + key] = '####'
                    if val == 'PRIVATE':
                        self.privateDict[section + '/' + key] = '####'
                        self.reversions[section + '/' + key] = 0
            self.reversions['public'] = 0

    def __str__(self):
        return f"Name: {self.name}\n" \
               f"Type: {self.type}\n"\
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

# 将需要改变的值写入文件
def write(change_dict: dict):
    return

def check_config(confgDict: dict) -> bool:
    # TODO:实现格式检查
    return True

def get_host():
    hostname = socket.gethostname()
    return socket.gethostbyname(hostname)

def parse_config(filename):
    with open(filename, 'r', encoding="UTF-8") as f:
        ext = filename.split('.')[-1]
        if ext == 'toml':
            return toml.load(f), 'toml'
        elif ext == 'ini' or ext == 'conf':
            config = configparser.ConfigParser()
            config.read_file(f)
            return config, 'conf'
        else:
            raise ValueError('Unsupported file format: {}'.format(ext))

def toml_write(parsed, tmpDict: dict, path):
    for key, value in tmpDict.items():
        if value != '####':
            section = 'DEFAULT'
            tl = key.split('/')
            if len(tl) > 1:
                section = tl[0]
            parsed[section][tl[-1]] = str(value)
    with open(path, 'w', encoding="UTF-8") as f:
        parsed.dump(parsed, f)

def conf_write(parsed, tmpDict: dict, path):
    for key, value in tmpDict.items():
        if value != '####':
            section = 'DEFAULT'
            tl = key.split('/')
            if len(tl) > 1:
                section = tl[0]
            parsed.set(section, tl[-1], str(value))
    with open(path, 'w', encoding="UTF-8") as f:
        parsed.write(f)

async def write_file(path, config: Config):
    try:
        async with asyncio.Lock():
            print('Try to process ')
            print(config)
            parsed, trans = parse_config(path)
            tmpDict = {}
            if config.type == InputType.PUBLIC:
                tmpDict = config.publicDict
            if config.type == InputType.PRIVATE:
                tmpDict = config.privateDict
            if config.type == InputType.SELFC:
                pass
            # TODO:文件处理
            if trans == 'toml':
                toml_write(parsed, tmpDict, path)
            elif trans == 'conf':
                conf_write(parsed, tmpDict, path)
    except FileNotFoundError:
        print(f'Error: File {path} not found')





async def process_queue():
    global WORKQUEUE
    while True:
        # 从队列中获取下一个元素，如果队列为空，则阻塞等待
        config = WORKQUEUE.get()
        print(config)
        # 处理元素

        # 写入文件
        await write_file(config.src, config)

        # 通知队列已处理完当前元素
        WORKQUEUE.task_done()