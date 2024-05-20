'''
Author       : Zhijie Yang
Date         : 2024-05-17 15:30:24
LastEditors  : Zhijie Yang
LastEditTime : 2024-05-19 01:58:58
FilePath     : /race/step_5_generate_history.py
Description  : 

Copyright (c) 2024 by Zhijie Yang, All Rights Reserved. 
'''
from gStore.src.GstoreConnector import GstoreConnector
from components.utils import load_json
from Levenshtein import distance
from tqdm import tqdm
from utils.parse_sparql_cwq import race_Parser
from utils.parse_sparql_webqsp import webqsp_Parser
from copy import deepcopy

import argparse
import json
import re
import os

ind = []


gc = GstoreConnector('localhost','9090', 'root', '123456')


def load_data(split, args):
    data_file_name = 'data/{}/sexpr/{}.expr.json'.format(args.dataset_type,split)
    print('Loading data from:',data_file_name)
    data_dict = load_json(data_file_name)
    return data_dict


def generate_history(question:str, sparql:str, entity_map:dict, dataset_type:str):
    # MAX_REL = 50
    pattern_str = r'ns:m\.0\w*'
    # pattern = re.compile(pattern_str)
    mid_list = list(set([mid.strip()
                    for mid in re.findall(pattern_str, sparql)]))
    if dataset_type == 'race':
        parser = race_Parser()

    try:
        g, s = parser.parse_query(sparql, mid_list)
    except:
        return []


    query_front_rel = "SELECT DISTINCT ?p WHERE {{   ?o ?p {en} . }}"
    query_front_en = "SELECT DISTINCT ?o WHERE {{   ?o {rel} {en} . }}"
    query_tail_rel = "SELECT DISTINCT ?p WHERE {{   {en} ?p ?o . }}"
    query_tail_en = "SELECT DISTINCT ?o WHERE {{   {en} {rel} ?o . }}"

    query = [f"[TASK]:Please output the query you want to do to generate a Logic Form Query. If no query is needed, please output Quit.\n\
[Question]:{question}\n[Golden Entity]:{list(entity_map.values()).__str__()}\n"]
    response = []



    rel = []
    entities = list(entity_map.values())
    entities = []
    for _ in g:
        s_exprs = _[1]
        for s_expr in s_exprs:
            en, q_en = "", ""
            query_rel, query_en = "", ""
            s_expr = [_.strip('<>') for _ in s_expr]
            if s_expr[0] in entity_map.keys() and (s_expr[0][0] != '?' or s_expr[2] not in entity_map.keys()) and s_expr[0] not in entities:
                en = s_expr[0]
                q_en = s_expr[2]
                query_rel = query_tail_rel
                query_en = query_tail_en
            elif s_expr[2] in entity_map.keys() and s_expr[2] not in entities:
                en = s_expr[2]
                q_en = s_expr[0]
                query_rel = query_front_rel
                query_en = query_front_en
            else:
                continue
            entities.append(en)
            rel.append(s_expr[1])

            en_list = []
            if en.startswith("?"):
                en_list = entity_map[en]
            else:
                en_list = [en]

            ret = []
            for _ in en_list:
                if '李相勋_（韩国组合100%原成员）' in en_list and _ != '李相勋_（韩国组合100%原成员）':
                    continue
                ql = query_rel.format(en = f"<{_}>")
                res = gc.query('race', 'json', ql)
                try:
                    res = [list(_.values())[0]['value'] for _ in json.loads(res)['results']['bindings']]
                except:
                    ql = query_rel.format(en = f'"{_}"')
                    res = gc.query('race', 'json', ql)
                    res = [list(_.values())[0]['value'] for _ in json.loads(res)['results']['bindings']]
                ret.extend(res)
            ret = list(set(ret))
            

            if (s_expr[1] not in ret):
                return []
            
            ret = sorted(ret, key=lambda x: distance(x, question), reverse=False)[:20]

            if (s_expr[1] not in ret):
                ret[19] = s_expr[1]

            response.append(f"{entity_map[s_expr[0]] if (s_expr[0] in entity_map.keys() and (s_expr[0][0] != '?' or s_expr[2] not in entity_map.keys())) else s_expr[0]} ?o \
{entity_map[s_expr[2]] if (s_expr[2] in entity_map.keys() and (s_expr[2][0] != '?' or s_expr[0] not in entity_map.keys())) else s_expr[2]}")
            q = f"[RELATIIONSHIP]:{{{entity_map[s_expr[0]] if (s_expr[0] in entity_map.keys() and (s_expr[0][0] != '?' or s_expr[2] not in entity_map.keys())) else s_expr[0]} ?o \
{entity_map[s_expr[2]] if (s_expr[2] in entity_map.keys() and (s_expr[2][0] != '?' or s_expr[0] not in entity_map.keys())) else s_expr[2]}:{ret}}}"
            query.append(q)


            ret = []
            for _ in en_list:
                ql = query_en.format(rel = f"<{s_expr[1]}>", en = f"<{_}>")
                res = gc.query('race', 'json', ql)
                try:
                    res = [list(_.values())[0]['value'] for _ in json.loads(res)['results']['bindings']]
                except:
                    ql = query_en.format(rel = f"<{s_expr[1]}>", en = f'"{_}"')
                    res = gc.query('race', 'json', ql)
                ret.extend(res)
            ret = [_.replace('http://rdf.freebase.com/ns/','') for _ in list(ret)]
            entity_map[q_en] = list(ret)


    response.append("Quit")
    history = []
    for i in range(len(response)):
        history.append((query[i], response[i]))

    return history


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset_type', default="race", type=str, help="CWQ | WebQSP")
    parser.add_argument('--kgmhp', default=True, type=bool)
    args = parser.parse_args()
    return args

def prepare_dataloader(args,split):
    assert split in ['train','test','dev','train_sample','dev_sample','test_sample']

    data = load_data(split, args)
    print(f'Origin {split} dataset len: {len(data)}')
    assert type(data)==list
    if 'train' in split or 'dev' in split:
        # for train and dev, filter the examples without sexpr
        examples = []
        for x in data:
            if x['SExpr'].lower()!="null":
                examples.append(x)                
    else:
        examples = [x for x in data]
    print(f'Real {split} dataset len: {len(examples)}')

    json_data=[]
    examples = examples[574:]
    instruction='Generate a Logical Form query that retrieves the information corresponding to the given question. \n'
    for cnt, item in tqdm(enumerate(examples), total = len(examples)):
        question=item['question']
        input = 'Question: { '+question+' }'       
        output = item['SExpr']
        item['gold_entity_map'] = {_:_ for _ in item['entities']}
        gold_en = item['gold_entity_map']
        raw_gold_en = deepcopy(gold_en)
        history = []
        if output != "null" and args.kgmhp:
            history = generate_history(item['question'], item['sparql'],item['gold_entity_map'], args.dataset_type)
        json_data.append({"instruction":instruction,"input":input,"output":output,"history":history,"gold_entity_map":raw_gold_en})

    output_dir = 'LLMs/data/{}_{}.json'.format(args.dataset_type, split)

    if not os.path.exists(os.path.dirname(output_dir)):
        os.mkdir(os.path.dirname(output_dir))   

    with open(output_dir, "w", encoding="utf-8") as file:
        json.dump(json_data, file, ensure_ascii=False, indent=4)    
    
    

if __name__=='__main__':
    
    args = _parse_args()
    print(args)
    # gc.load('race')
    prepare_dataloader(args,'train')
    # gc.unload('race')
    # prepare_dataloader(args, 'test')
    print('Finished')

