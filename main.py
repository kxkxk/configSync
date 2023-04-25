from configparser import ConfigParser
import toml
from .util import *

CHANGE_TAG = '_NEEDSYNC_'
SELFCONFPATH = '/etc/synconfig'

# 读配置，获取当前需要同步的文件,生成监视对象加入线程池
# 监视etcd相关key改动，改动version高于当前version时更新并同步配置
# 分为3种，public改动全部接受，private改动只接受前缀hostname与本机相同的
# etcd集群配置改动后重启服务


## 基础配置：



# 监视文件改动，读配置文件,编成列表




# 创建watch，更新配置

# if __name__== "__main__" :
#     config_dict = read_self_config(SELFCONFPATH)
#     for i in config_dict['configs']:
