# -*- coding:utf-8 -*-
from rabbiteq import Consumer
import time

c = Consumer('127.0.0.1', 'wigger', 'wdxzdd', 1)


def callback(ch, method, properties, body):
    print('开始处理消息')
    time.sleep(len(body))
    print(f'消息内容：{body}')
    print('结束任务')


c.start_work(callback=callback)
