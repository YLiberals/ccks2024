'''
Author       : Zhijie Yang
Date         : 2024-04-18 13:27:00
LastEditors  : Zhijie Yang
LastEditTime : 2024-05-19 00:30:11
FilePath     : /race/test.py
Description  : 

Copyright (c) 2024 by Zhijie Yang, All Rights Reserved. 
'''
# import json
# from tqdm import tqdm

# data = json.load(open('data/race/scheme/entities.json'))

# for entity in tqdm(data):
#     if '".' in entity:
#         print(entity)

# import json
# from gStore.src.GstoreConnector import GstoreConnector

# gc = GstoreConnector('localhost','9090', 'root', '123456')
# gc.unload('race')
# res = gc.load('race')

# ql = """SELECT DISTINCT ?p\nWHERE {\n  <莫妮卡·贝鲁奇> ?p ?o .\n}"""

# res = gc.query('race', 'json', ql)

# res = [list(_.values())[0]['value'] for _ in json.loads(res)['results']['bindings']]

# print(res)
