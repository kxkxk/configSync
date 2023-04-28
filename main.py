from configparser import ConfigParser
from util import *
from watch import *
import time

# TODO: 1. 使用命令行更新配置
#       2. 延迟更新监视的etcd节点
#       3. 读取失败时不更新文件
#       4. 加日志

def watch_public(watcher: EtcdWatcher, config: Config, prifix):
    print('now watch: ' + prifix + config.name)
    watcher.create_watcher(prifix + config.name, callback=watch_public_callback, prefix=True)

def watch_private(watcher: EtcdWatcher, config: Config, prifix):
    # private_dict: {'section/'}
    for i in config.privateDict.keys():
        print('now watch: ' + prifix + HOST + '/' + config.name + '/' + i)
        watcher.create_watcher(prifix + HOST + '/' + config.name + '/' + i, callback=watch_private_callback, prefix=False)


# 创建watch，更新配置

def run_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_until_complete(process_queue())

def main():
    global CONFIGTABLE
    global HOST
    global WORKQUEUE
    config_dict, _ = parse_config(SELFCONFPATH)
    publicPrifix = config_dict['public_prifix']
    privatePrifix = config_dict['private_prifix']
    # 初始化watcher
    watcher = EtcdWatcher(config_dict['nodes'])
    # 初始化工作队列
    WORKQUEUE = queue.Queue()
    if HOST == None:
        HOST = get_host()
    # 根据配置文件初始化监听
    for i in config_dict['configs']:
        tmpDict, _ = parse_config(i)
        # TODO: 检查格式
        if not check_config(tmpDict):
            print('有问题')
            continue
        name = tmpDict['basic_settings']['name']
        src = tmpDict['basic_settings']['src']
        tmpConfig = Config(name, src)
        tmpConfig.set_dict(tmpDict)
        CONFIGTABLE[name] = tmpConfig
        print(tmpConfig)
        watch_public(watcher, tmpConfig, publicPrifix)
        watch_private(watcher, tmpConfig, privatePrifix)

        loop = asyncio.new_event_loop()
        t = threading.Thread(target=run_loop, args=(loop,))
        t.start()
    while True:
        time.sleep(1)
        pass

if __name__== "__main__" :
    main()
    # 开一个守护线程，一个修改线程，一个读线程(之后搞cmd用)

# {"etcd/hosts": ["10.0.0.71:7480", "10.0.0.27:7480"], "grpc/port": 50052}