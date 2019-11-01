#!/usr/bin/env python3

import pprint
import collections
from Chinook_Python import *


def project(relation, columns):
    # relation:  set
    # columns: list of strings as column names
    result = []
    all_columns = next(iter(relation))._fields
    tuple_name = next(iter(relation)).__repr__().split("(")[0]
    tuple_ = collections.namedtuple(tuple_name,columns)
    for i in range(len(relation)):
        # get the values for each field
        values = [list(relation)[i]._asdict()[x] for x in columns if x in all_columns]
        result.append(tuple_(*values))
    return set(result)


def select(relation, predicate):
    result = [list(relation)[i] for i in range(len(relation)) if predicate(list(relation)[i])]
    return set(result)


def rename(relation, new_columns=None, new_relation=None):
    result = []
    all_columns = list(next(iter(relation))._asdict().keys())
    tuple_name = next(iter(relation)).__repr__().split("(")[0]
    tuple_ = collections.namedtuple(tuple_name, new_columns)
    for i in range(len(relation)):
        values = [list(relation)[i]._asdict()[j] for j in all_columns]
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
    for i in range(len(relation1)):
        for j in range(len(relation2)):
            values1 = list(list(relation1)[i].__getnewargs__())
            values2 = list(list(relation2)[j].__getnewargs__())
            result.append(tuple_(*(values1 + values2)))
    return set(result)


def theta_join(relation1, relation2, predicate):
    pass


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
