#!/usr/bin/env python3

import pprint
import collections
from Chinook_Python import *


def project(relation, columns):
    # relation:  set
    # columns: list of strings as column names
    result = []
    all_columns = list(relation)[0]._fields
    tuple_ = collections.namedtuple("result", columns)
    for i in range(len(relation)):
        values = []
        for j in range(len(columns)):
            if columns[j] in all_columns:
                values.append(list(relation)[i]._asdict()[columns[j]])
        result.append(tuple_(*values))
    result = set(result)
    return result


def select(relation, predicate):
    result = []
    all_columns = list(relation)[0]._fields
    tuple_ = collections.namedtuple("result", all_columns)
    for i in range(len(relation)):
        if predicate(list(relation)[i]):
            values = []
            for j in all_columns:
                values.append(list(relation)[i]._asdict()[j])
            result.append(tuple_(*values))
    result = set(result)
    return result


def rename(relation, new_columns=None, new_relation=None):
    pass


def cross(relation1, relation2):
    pass


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
