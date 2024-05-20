'''
Author       : Zhijie Yang
Date         : 2024-05-17 01:44:44
LastEditors  : Zhijie Yang
LastEditTime : 2024-05-17 02:46:42
FilePath     : /race/data_format.py
Description  : 将数据类型进行封装

Copyright (c) 2024 by Zhijie Yang, All Rights Reserved. 
'''
from dataclasses import field, asdict, dataclass
from typing import List, Tuple, Any, Dict


@dataclass
class DataFormat:
    question_id : str = field(
        metadata={
            "help": "问题ID"
        },
    )

    question : str = field(
        metadata={
            "help": "问题"
        },
    )

    sparql : str = field(
        default=None,
        metadata={
            "help": "sparql"
        },
    )

    answer : List[str] = field(
        default=None,
        metadata={
            "help": "答案"
        },
    )

    entities : List[str] = field(
        default=None,
        metadata={
            "help": "实体集合"
        },
    )

    history : List[Tuple[str,str]] = field(
        default=None,
        metadata={
            "help": "历史对话"
        },
    )

    def __init__(self, qid:str, q:str, a:List[str], s, e:List[str], h:List[Tuple[str,str]]) -> None:
        self.question_id = qid
        self.question = q
        self.sparql = s
        self.answer = a
        self.entities = e
        self.history = h
        pass

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
        return {
            "question": self.question,
            "answer": self.answer,
            "sparql": self.sparql,
            "entities": self.entities,
            "history": self.history
        }