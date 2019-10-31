#!/usr/bin/env python3

import pprint

from Chinook_Python import *


def project(relation, columns):
    pass


def select(relation, predicate):
    pass


def rename(relation, new_columns=None, new_relation=None):
    pass


def cross(relation1, relation2):
    pass


def theta_join(relation1, relation2, predicate):
    pass


def natural_join(relation1, relation2):
    pass


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
