#!/usr/bin/env python3

import pprint
import collections
from Chinook_Python import *


def project(relation, columns):
    result = []
    all_columns = next(iter(relation))._fields
    tuple_name = next(iter(relation)).__repr__().split("(")[0]
    tuple_ = collections.namedtuple(tuple_name, columns)
    iter_ = iter(relation)
    for i in range(len(relation)):
        #get the values for each column field
        record = next(iter_)
        values = [record._asdict()[x] for x in columns if x in all_columns]
        result.append(tuple_(*values))
    return set(result)


def select(relation, predicate):
    result = [list(relation)[i] for i in range(len(relation)) if predicate(list(relation)[i])]
    return set(result)


def rename(relation, new_columns=None, new_relation=None):
    result = []
    all_columns = list(next(iter(relation))._asdict().keys())
    tuple_name = next(iter(relation)).__repr__().split("(")[0]
    tuple_ = collections.namedtuple(tuple_name,new_columns)
    iter_ = iter(relation)
    for i in range(len(relation)):
        record = next(iter_)
        values = [record._asdict()[j] for j in all_columns]
        result.append(tuple_(*values))
    return set(result)


def cross(relation1, relation2):
    rel1_columns = list(next(iter(relation1))._fields)
    rel2_columns = list(next(iter(relation2))._fields)
    rel1_name = next(iter(relation1)).__repr__().split("(")[0]
    rel2_name = next(iter(relation2)).__repr__().split("(")[0]
    # rename same column names
    new_rel1_columns = rel1_columns.copy()
    new_rel2_columns = rel2_columns.copy()
    new_rel1_columns = [(rel1_name + "_" + new_rel1_columns[i]) if (new_rel1_columns[i] in rel2_columns) else new_rel1_columns[i] for i in range(len(new_rel1_columns))]
    new_rel2_columns = [(rel2_name + "_" + new_rel2_columns[i]) if (new_rel2_columns[i] in rel1_columns) else new_rel2_columns[i] for i in range(len(new_rel2_columns))]
    tuple_ = collections.namedtuple("result", new_rel1_columns+new_rel2_columns)
    result = []
    iter1 = iter(relation1)
    for i in range(len(relation1)):
        tuple1 = next(iter1)
        values1 = list(tuple1.__getnewargs__())
        iter2 = iter(relation2)
        for j in range(len(relation2)):
            tuple2 = next(iter2)
            values2 = list(tuple2.__getnewargs__())
            result.append(tuple_(*(values1+ values2)))
    return set(result)


def theta_join(relation1, relation2, predicate):
    rel1_columns = list(next(iter(relation1))._fields)
    rel2_columns = list(next(iter(relation2))._fields)
    tuple_ = collections.namedtuple("result", rel1_columns+rel2_columns)
    result = []
    iter1 = iter(relation1)
    for i in range(len(relation1)):
        dict1 = next(iter1)
        iter2 = iter(relation2)
        result.append(dict1._asdict())
        for j in range(len(relation2)):
            dict2 = next(iter2)
            keys2 = list(dict2._asdict())
            if predicate(dict1, dict2):
                for k in rel2_columns:
                    result[i][k] = dict2._asdict()[k]
        result[i] = tuple_(**result[i])
    return set(result)


def natural_join(relation1, relation2):
    pass


"""
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

pprint.pprint(
    project(
        natural_join(
            Album,
            select(Artist, lambda t: t.Name == 'Red Hot Chili Peppers')
        ),
        ['Title']
    )
)
"""


pprint.pprint(project(Artist, ["Name"]))
pprint.pprint(select(Artist, lambda t: t.Name == "Red Hot Chili Peppers"))
pprint.pprint(rename(Album,["Id", "NameofAlbum", "Artist"]))
pprint.pprint(len(cross(Artist, Album)))
pprint.pprint(theta_join(Album, rename(Artist, ['Id', 'Name']), lambda t1, t2: t1.ArtistId == t2.Id))
