# -*- coding:utf-8 -*-
from rabbiteq import Producer

q = Producer('127.0.0.1', 'wigger', 'wdxzdd')
q.init_queue()
q.put('aaaaaaaaaaa')
q.close()