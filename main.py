from configparser import ConfigParser
from util import *
from watch import *
import time

# 读配置，获取当前需要同步的文件,生成监视对象加入线程池
# 监视etcd相关key改动，改动version高于当前version时更新并同步配置
# 分为3种，public改动全部接受，private改动只接受前缀hostname与本机相同的
# etcd集群配置改动后重启服务

# 监视文件改动，读配置文件,编成列表

def watch_public(watcher: EtcdWatcher, config: Config, prifix):
    print('now watch: ' + prifix + config.name)
    watcher.create_watcher(prifix + config.name, callback=watch_public_callback, prefix=True)

def watch_private(watcher: EtcdWatcher, config: Config, prifix):
    # private_dict: {'section/'}
    for i in config.privateDict.keys():
        print('now watch: ' + prifix + HOST + '/' + config.name + '/' + i)
        watcher.create_watcher(prifix + HOST + '/' + config.name + '/' + i, callback=watch_private_callback, prefix=False)


# 创建watch，更新配置

def main():
    global CONFIGTABLE
    global HOST
    config_dict = load_config(SELFCONFPATH)
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
        tmpDict = load_config(i)
        # TODO: 检查格式
        if not check_config(tmpDict):
            print('有问题')
            continue
        name = tmpDict['basic_settings']['name']
        src = tmpDict['basic_settings']['src']
        tmpConfig = Config(name, src)
        tmpConfig.set_dict(tmpDict)
        CONFIGTABLE[name] = tmpConfig
        watch_public(watcher, tmpConfig, publicPrifix)
        watch_private(watcher, tmpConfig, privatePrifix)
    while True:
        time.sleep(1)
        pass

if __name__== "__main__" :
    main()
    # 开一个守护线程，一个修改线程，一个读线程(之后搞cmd用)