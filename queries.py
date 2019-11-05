#!/usr/bin/env python3

import pprint
import collections
import math
from Chinook_Python import *


def project(relation, columns):
    all_columns = next(iter(relation))._fields
    tuple_name = next(iter(relation)).__repr__().split("(")[0]
    tuple_ = collections.namedtuple(tuple_name, columns)
    iter_ = iter(relation)
    iterations = math.ceil(len(relation)/10000)
    result_list = [[] for i in range(iterations)]
    for i in range(len(relation)):
        #get the values for each column field
        record = next(iter_)
        values = [record._asdict()[x] for x in columns if x in all_columns]
        result_list[math.floor(i/10000)].append(tuple_(*values))
    result = [k for m in result_list for k in m]
    print("Cardinality of project: ",len(result))
    return set(result)


def select(relation, predicate):
    iterations = math.ceil(len(relation) / 10000)
    rel_batch_list = []
    result_list = []
    for i in range(iterations):
        start = i * 10000
        end = (i + 1) * 10000
        rel_batch_list = list(relation)[start:end]
        result_list.append([j for j in rel_batch_list if predicate(j)])

    result = [k for m in result_list for k in m]
    print("Cardinality of select: ", len(result))
    return set(result)


def rename(relation, new_columns=None, new_relation=None):
    result = []
    all_columns = list(next(iter(relation))._asdict().keys())
    tuple_name = next(iter(relation)).__repr__().split("(")[0]
    tuple_ = collections.namedtuple(tuple_name, new_columns)
    iter_ = iter(relation)
    iterations = math.ceil(len(relation)/10000)
    result_list = [[] for i in range(iterations)]
    for i in range(len(relation)):
        record = next(iter_)
        values = [record._asdict()[j] for j in all_columns]
        result_list[math.floor(i/10000)].append(tuple_(*values))
    result = [k for m in result_list for k in m]
    print("Cardinality of rename: ",len(result))
    return set(result)

#still the fastest of them all
def cross(relation1, relation2):
    rel1_columns = list(next(iter(relation1))._fields)
    rel2_columns = list(next(iter(relation2))._fields)
    rel1_name = next(iter(relation1)).__repr__().split("(")[0]
    rel2_name = next(iter(relation2)).__repr__().split("(")[0]
    #rename same column names
    new_rel1_columns = rel1_columns.copy()
    new_rel2_columns = rel2_columns.copy()
    new_rel1_columns = [(rel1_name + "_" + new_rel1_columns[i]) if (new_rel1_columns[i] in rel2_columns) else new_rel1_columns[i] for i in range(len(new_rel1_columns))]
    new_rel2_columns = [(rel2_name + "_" + new_rel2_columns[i]) if (new_rel2_columns[i] in rel1_columns) else new_rel2_columns[i] for i in range(len(new_rel2_columns))]
    tuple_ = collections.namedtuple("result", new_rel1_columns + new_rel2_columns)
    iterations = math.ceil((len(relation1) * len(relation2)) / 10000)
    result_list = [[] for i in range(iterations)]
    iter1 = iter(relation1)
    for i in range(len(relation1)):
        record1 = next(iter1)
        values1 = list(record1.__getnewargs__())
        iter2 = iter(relation2)
        for j in range(len(relation2)):
            record2 = next(iter2)
            values2 = list(record2.__getnewargs__())
            result_list[math.floor(i / 10000)].append(tuple_(*(values1 + values2)))
    result = [k for m in result_list for k in m]
    print("Cardinality of cross: ",len(result))
    return set(result)


def theta_join(relation1, relation2, predicate):
    rel1_columns = list(next(iter(relation1))._fields)
    rel2_columns = list(next(iter(relation2))._fields)
    tuple_ = collections.namedtuple("result", rel1_columns + rel2_columns)
    #result = []
    iterations = math.ceil((len(relation1) * len(relation2)) / 10000)
    result_list = [[] for i in range(iterations)]
    iter1 = iter(relation1)
    for i in range(len(relation1)):
        record1 = next(iter1)
        iter2 = iter(relation2)
        for j in range(len(relation2)):
            record2 = next(iter2)
            if predicate(record1, record2):
                resultdict = record1._asdict()
                for k in rel2_columns:
                    resultdict[k] = record2._asdict()[k]
                result_list[math.floor(i / 10000)].append(tuple_(**resultdict))
                break;
    result = [k for m in result_list for k in m]
    print("Cardinality of theta join: ",len(result))
    return set(result)


def natural_join(relation1, relation2):
    rel1_columns = list(next(iter(relation1))._fields)
    rel2_columns = list(next(iter(relation2))._fields)
    common_col = None
    for i in rel1_columns:
        if i in rel2_columns:
            common_col = i
    if (common_col == None):
        raise ValueError("No common Column found")
    all_columns = list(set(rel1_columns + rel2_columns))
    tuple_ = collections.namedtuple("result", all_columns)
    #result = []
    iterations = math.ceil((len(relation1) * len(relation2)) / 10000)
    result_list = [[] for i in range(iterations)]
    iter1 = iter(relation1)
    for i in range(len(relation1)):
        record1 = next(iter1)
        iter2 = iter(relation2)
        for j in range(len(relation2)):
            record2 = next(iter2)
            if record1._asdict()[common_col] == record2._asdict()[common_col]:
                resultdict = record1._asdict()
                for k in rel2_columns:
                    if k != common_col:
                        resultdict[k] = record2._asdict()[k]
                #result.append(tuple_(**resultdict))
                result_list[math.floor(i / 10000)].append(tuple_(**resultdict))
    result = [k for m in result_list for k in m]
    print("Cardinality of natural join: ",len(result))
    return set(result)

print("\n\nQUERY 1\n")
pprint.pprint(
    project(
        select(
            select(
                cross(
                    Album,
                    rename(Artist, ['Id', 'Name'])
                ),
                lambda t: t.ArtistId == t.Id
            ),
            lambda t: t.Name == 'Red Hot Chili Peppers'
        ),
        ['Title']
    )
)

print("\n\nQUERY 2\n")
pprint.pprint(
    project(
        select(
            theta_join(
                Album,
                rename(Artist, ['Id', 'Name']),
                lambda t1, t2: t1.ArtistId == t2.Id
            ),
            lambda t: t.Name == 'Red Hot Chili Peppers'
        ),
        ['Title']
    )
)

print("\n\nQUERY 3\n")
pprint.pprint(
    project(
        theta_join(
            Album,
            rename(
                select(Artist, lambda t: t.Name == 'Red Hot Chili Peppers'),
                ['Id', 'Name']
            ),
            lambda t1, t2: t1.ArtistId == t2.Id
        ),
        ['Title']
    )
)

print("\n\nQUERY 4\n")

pprint.pprint(
    project(
        natural_join(
            Album,
            select(Artist, lambda t: t.Name == 'Red Hot Chili Peppers')
        ),
        ['Title']
    )
)


#OLD QUERY
print("\n\nQUERY 5 (Project 1 query 1)\n")
pprint.pprint(project(select(natural_join(Album, Artist), lambda t: t.Name == 'Red Hot Chili Peppers'),['Title']))


#NEW QUERY
print("\n\nOptimized QUERY 5 (Project 1 query 1)\n")
pprint.pprint(project(natural_join(Album, select(Artist, lambda t: t.Name == 'Red Hot Chili Peppers')), ['Title']))
