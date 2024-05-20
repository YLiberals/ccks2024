'''
Author       : Zhijie Yang
Date         : 2024-05-18 23:44:12
LastEditors  : Zhijie Yang
LastEditTime : 2024-05-19 00:11:18
FilePath     : /race/step_4_build_dataset.py
Description  : 构造gstore图数据库

Copyright (c) 2024 by Zhijie Yang, All Rights Reserved. 
'''

from gStore.src.GstoreConnector import GstoreConnector

gc = GstoreConnector('localhost','9090', 'root', '123456')

#docker下路径
# res = gc.build('race', '/app/triple.txt')
res = gc.show()

print(res)