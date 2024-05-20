'''
Author       : Zhijie Yang
Date         : 2024-05-16 19:17:32
LastEditors  : Zhijie Yang
LastEditTime : 2024-05-16 20:29:21
FilePath     : /race/step_1_extract_entities_and_relationships.py
Description  : 

Copyright (c) 2024 by Zhijie Yang, All Rights Reserved. 
'''
import os
import json


from tqdm import tqdm
from copy import deepcopy
from typing import List, Tuple


def extract_entities_and_relationships(data: str) -> Tuple[List[str]]:
    '''
    description: 提取实体和关系\n
    param {str} data 数据\n
    return {*} 返回实体、关系列表
    '''
    entities = []
    relationships = []
    data = data.strip('\n. ')
    try:
        h, r, t = data.split('\t')
    except:
        return [],[]
    
    if h[0] == '<' and h[-1] == '>':
        entities.append(h[1:-1])
    if t[0] == '<' and t[-1] == '>':
        entities.append(t[1:-1])
    if r[0] == '<' and r[-1] == '>':
        relationships.append(r[1:-1])
    return entities,relationships


def test():
    s = '<XD> <缩写>\t"Exclud_Dividend" .\n'
    print(extract_entities_and_relationships(s))


def main():
    root_path = "data/triple.txt"
    target_path = "data/race/scheme"

    if not os.path.exists(target_path):
        os.mkdir(target_path)

    entities, relationships = [], []

    for data in tqdm(open(root_path,'r')):
        en, r = extract_entities_and_relationships(data)
        entities.extend(en)
        relationships.extend(r)

    entities = list(set(entities))
    relationships = list(set(relationships))

    with open(os.path.join(target_path,'entities.json'),'w') as f:
        json.dump(entities,f)

    with open(os.path.join(target_path,'relationships.json'),'w') as f:
        json.dump(relationships,f)

    print("Get {} entities and {} relationships".format(len(entities),len(relationships)))

if __name__ == "__main__":
    # test()
    main()
    print('Done!')
