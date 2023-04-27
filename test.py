import etcd3
from util import *
from watch import *
import time
import threading
import grpc
import socket
import queue
import asyncio

# def get_watch(key, callback):
#     etcd_client = EtcdClient(["10.50.7.24:2379"], 5).pick_up()
#     watch_id = etcd_client.add_watch_prefix_callback(key, callback=callback)
#     return watch_id

# def watch_callback(response):
#     print('start')
#     print(response.header.revision)
#     for i in response.events:
#         print(i)
#     return 

# def wait_for_callback():
#     event = threading.Event()
#     event.wait()

# watch_id = get_watch("/my",watch_callback)
# watch_id2 = get_watch("/me",watch_callback) 
# t = threading.Thread(target=wait_for_callback)
# t.start()

async def write_file(filename, value):
    async with asyncio.Lock():
        with open(filename, 'a') as f:
            f.write(value + '\n')

async def modify_worker(filename):
    while True:
        try:
            # 从队列中取出需要修改的值
            value = q.get(timeout=1)
        except queue.Empty:
            # 如果队列为空，等待一段时间再尝试取值
            time.sleep(0.1)
            continue

        if value is None:
            # 如果队列中取出 None，表示队列已经被清空，退出循环
            break

        await write_file(filename, value)
        # 处理完一个值后，向队列发送信号
        q.task_done()

# 创建一个队列
q = queue.Queue()

# 创建一个修改线程，传入文件名
worker_thread = threading.Thread(target=modify_worker, args=('data.txt'))
worker_thread.start()

# 向队列中放入需要修改的值
for i in range(10):
    q.put('Value {}'.format(i))

# 队列中放入 None，表示队列已经被清空
q.put(None)

# 等待队列中的所有值被处理完毕
q.join()

# 等待修改线程退出
worker_thread.join()

# 主线程继续执行
print('Done')