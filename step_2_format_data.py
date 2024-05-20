'''
Author       : Zhijie Yang
Date         : 2024-05-16 19:11:58
LastEditors  : Zhijie Yang
LastEditTime : 2024-05-19 22:00:18
FilePath     : /race/step_2_format_data.py
Description  : 第二步，格式化数据

Copyright (c) 2024 by Zhijie Yang, All Rights Reserved. 
'''
from components.utils import extract_mentioned_entities_from_sparql
from data_format import DataFormat
from typing import List, Tuple
from copy import deepcopy
from tqdm import tqdm

import numpy as np
import json
import os
import re

class DataFormater:

    def __init__(self, root_dir:str='data/race/origin') -> "None":
        self.root_dir = root_dir
        self.train_path = os.path.join(root_dir, 'train.txt')
        self.test_path = os.path.join(root_dir, 'test.txt')
        pass


    def precess_data(self) -> "Tuple[List[DataFormat],List[DataFormat]]":
        self.train_data = self.split_train_data(self.train_path)
        self.test_data = self.split_test_data(self.test_path)
        return self.train_data, self.test_data


    def find_union_clauses(self, sparql:str) -> "List[str]":
        '''
        description: 查找sparql中的union子句\n
        param {str} sparql sparql字符串\n
        return {*} union子句
        '''
        ret = []
        ids = 0
        while ids < len(sparql):
            if sparql[ids] == '{':
                sub_clauses = ""
                sub_clauses += '{'
                sum = 1
                while sum != 0:
                    ids += 1
                    if sparql[ids] == '{':
                        sum += 1
                    elif sparql[ids] == '}':
                        sum -= 1
                    sub_clauses += sparql[ids]
                ret.append(sub_clauses)
            else:
                ids += 1

        return ret


    def sparql_format(self, sparql:str) -> "str":
        '''
        description: 格式化sparql\n
        param {str} sparql sparql字符串\n
        return {*} 格式化后的sparql
        '''
        sparql = sparql.replace('\n', '')
        sparql = sparql.replace(' distinct ', ' ')
        sparql = sparql.strip()
        try:
            result_alias = re.findall(r"select\s*?(\?\w+)\s*?", sparql.lower(), re.DOTALL)[0]
        except:
            return sparql
        if '?x' in sparql and result_alias != '?x':
            sparql = sparql.replace('?x', '<|im_placeholder|>')
        sparql = sparql.replace(result_alias, '?x')
        if '<|im_placeholder|>' in sparql:
            sparql = sparql.replace('<|im_placeholder|>', result_alias)
        sparql = sparql.replace('select ?x', 'SELECT ?x\n')
        # sparql = sparql[:-1].strip() + " \n}"

        if len(re.findall(r'\}\s?union\s?\{', sparql.lower(), re.DOTALL)) > 0:
            pattern = r"\{(.+)\}"
            union_clauses = self.find_union_clauses(re.findall(pattern,sparql)[0])
            st = sparql.find(union_clauses[0])
            ed = sparql.find(union_clauses[-1]) + len(union_clauses[-1])
            sparql = sparql.replace(sparql[st:ed], '<|im_clauses|>')

        # 结尾加上 点
        if sparql[:-1].strip()[-1] != '.':
            if sparql[:-1].strip()[-1] != '}':
                sparql = sparql[:-1].strip() + ".}"
        # 换行
        if '<|im_clauses|>' not in sparql:
            clauses = re.findall(r"\{(.+)\}", sparql, re.DOTALL)[0]
            en_and_rel = self.extract_clauses(clauses)
            lines = []
            for  i, _ in enumerate(en_and_rel):
                lines.append(" ".join(_) + " .")
            sparql = sparql.replace(clauses, "\n"+'\n'.join(lines)+'\n')


        # FILTER后面加空格
        matched = re.findall(r"\{(.+)\}", sparql, re.DOTALL)[0].strip()
        row = matched.split('\n')
        for i, _ in enumerate(row):
            if _.strip().lower().startswith('filter') and _.strip()[7] != ' ':
                row[i] = "FILTER " + row[i][7:]
        row = "\n".join(row)
        sparql = sparql.replace(matched, row)

        # 把regex(\?., .*?)替换为regex(str(\?., .*?))
        sparql = re.sub(r'regex\((\?\w+),\s(.*?)\)', r'REGEX(str(\1), \2)', sparql)
        sparql = re.sub(r'REGEX\((\?\w+),\s(.*?)\)', r'REGEX(str(\1), \2)', sparql)


        # 将clauses替换回来
        if '<|im_clauses|>' in sparql:
            sparql = sparql.replace('<|im_clauses|>', f"{' UNION '.join(union_clauses)}")

        sparql = sparql[:-1].strip() + "\n}"
        sparql = re.sub(r'\swhere\s?\{', 'WHERE {\n', sparql)
        return sparql


    def extract_clauses(self, clause:str) -> "List[str]":
        '''
        description: 
        param {str} sparql
        return {*}
        '''
        ret = []
        ids = 0
        l = len(clause)
        triple = []
        while ids < l:
            entity = ""
            if clause[ids] == '<':
                while ids < l and clause[ids] != '>':
                    entity += clause[ids]
                    ids += 1
                entity += '>'
                triple.append(entity)
            elif clause[ids] == '"':
                entity = '"'
                ids += 1
                while ids < l and clause[ids] != '"':
                    entity += clause[ids]
                    ids += 1
                entity += '"'
                ids += 1
                triple.append(entity)
            elif clause[ids] == '?':
                entity = "?"
                ids += 1
                while ids < l and clause[ids].isalpha():
                    entity += clause[ids]
                    ids += 1
                triple.append(entity)
            elif clause[ids:].lower().startswith('filter'):
                while ids < l and clause[ids] != '.':
                    entity += clause[ids]
                    ids += 1
                triple = []
            else:
                ids += 1
            
            if len(triple) == 3:
                ret.append(triple)
                triple = []
            elif len(triple) == 0 and entity != '':
                ret.append([entity])

        return ret


    def extract_entities_and_rel(self, clause:str) -> "List[str]":
        '''
        description: 提取实体和关系三元组\n
        param {str} sparql sparql字符串\n
        return {*} 返回实体和关系三元组
        '''
        ret = []
        ids = 0
        l = len(clause)
        while ids < l:
            if clause[ids] == '<':
                entity = ""
                while ids < l and clause[ids] != '>':
                    entity += clause[ids]
                    ids += 1
                entity += '>'
                ret.append(entity)
            elif clause[ids] == '"':
                entity = '"'
                ids += 1
                while ids < l and clause[ids] != '"':
                    entity += clause[ids]
                    ids += 1
                entity += '"'
                ids += 1
                ret.append(entity)
            elif clause[ids] == '?':
                entity = "?"
                ids += 1
                while ids < l and clause[ids].isalpha():
                    entity += clause[ids]
                    ids += 1
                ret.append(entity)
            else:
                ids += 1

        ret = np.array(ret).reshape(-1,3)
        return ret

    def simplify_sparql(self, sparql:str) -> "str":
        '''
        description: 对sparql进行处理，保证其中每一行都包含变量\n
        param {str} sparql 格式化后的sparql\n
        return {*} 返回sparql\n
        '''
        if 'union' in sparql.lower():
            return sparql
        clauses = re.findall(r'\{(.+)\}', sparql, re.DOTALL)[0]
        clauses = clauses.strip().split('\n')

        ret = []
        for i, _ in enumerate(clauses):
            if _.lower().startswith('filter'):
                ret.append(_)
                continue
            h, r, t = self.extract_entities_and_rel(_)[0]
            if h[0] == '?' or t[0] == '?' or r[0] == '?':
                ret.append(" ".join([h,r,t]) + " .")

        ret = '\n'+'\n'.join(ret)+'\n'
        raw = re.findall(r'\{(.+)\}', sparql, re.DOTALL)[0]
        sparql = sparql.replace(raw, ret)
        return sparql


    def split_train_data(self, path:str) -> "List[DataFormat]":
        '''
        description: 将数据按条进行切割\n
        param {str} path 数据路径\n
        return {*} 返回一个List代表所有数据
        '''
        ret = []
        q, a, s = "","",""
        ids = 1
        with open(path, 'r') as f:
            data = f.readlines()
            i = 0
            for i in tqdm(range(0,len(data),4)):
                q = data[i].strip()
                q = q.replace(f"q{ids}:",'')
                s = self.sparql_format(data[i+1].strip())
                try:
                    s = self.simplify_sparql(s)
                except:
                    pass
                a = data[i+2].strip().split('\t')
                e = extract_mentioned_entities_from_sparql(re.findall(r'\{(.+)\}', s, re.DOTALL)[0].strip('\n'))
                # TODO: 增加entities候选集
                if ids != 60 and ids != 506:
                    ret.append(DataFormat(f"q{ids}",q,a,s,e,[]).to_dict())
                ids += 1

        return ret


    def split_test_data(self, path:str) -> "List[DataFormat]":
        '''
        description: 将数据按条进行切割\n
        param {str} path 数据路径\n
        return {*} 返回一个List代表所有数据
        '''
        ret = []
        q = ""
        ids = 1
        with open(path, 'r') as f:
            data = f.readlines()
            i = 0
            for i in range(0,len(data)):
                q = data[i].strip('\n')
                q = q.replace(f"q{ids}:",'')
                ret.append(DataFormat(f"q{ids}",q,'','',[],[]).to_dict())
                ids += 1

        return ret


def test(path:str):
    df = DataFormater(path)
    ret = df.sparql_format('select ?z where { ?x <公司名称> ?y . filter regex(?y, "本田") . ?x <主要车型> ?z . }')
    ret = df.simplify_sparql(ret)
    # e = extract_mentioned_entities_from_sparql(re.findall(r'\{(.+)\}', ret, re.DOTALL)[0].strip('\n'))
    print(ret)
    # print(e)


def main():
    root_dir = 'data/race/origin'
    target_dir = 'data/race/format'
    df = DataFormater(root_dir)
    train, test = df.precess_data()

    if not os.path.exists(target_dir):
        os.mkdir(target_dir)

    with open(os.path.join(target_dir, 'train.json'), 'w') as f:
        json.dump(train, f, indent=4, ensure_ascii=False)

    with open(os.path.join(target_dir, 'test.json'), 'w') as f:
        json.dump(test, f, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    # test('data/race/origin')
    main()
    print('Done!')