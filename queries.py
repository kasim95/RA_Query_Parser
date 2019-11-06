#!/usr/bin/env python3
# CPSC 531 Project 2
# Mohammed Kasim Panjri
# Nirav Patil

import pprint
import collections
import math
from Chinook_Python import *

# All the functions are implemented such that they create result as list of lists and then convert it into sets
# The length of each inner list is set to 10000
# Such an implementation for select decreased the time required for query 1 from 47 seconds to 300 milliseconds
# So we implemented all the functions likewise


def project(relation, columns):
    all_columns = next(iter(relation))._fields                              # get all column names
    tuple_name = next(iter(relation)).__repr__().split("(")[0]              # get input tuple name
    tuple_ = collections.namedtuple(tuple_name, columns)                    # create namedtuple with same name as input
    iter_ = iter(relation)                                                  # iterator for input relation
    iterations = math.ceil(len(relation)/10000)                             # calc no of inner lists required
    result_list = [[] for i in range(iterations)]  # create list of lists with inner list restricted to 10000 length
    for i in range(len(relation)):
        record = next(iter_)
        values = [record._asdict()[x] for x in columns if x in all_columns]  # get the values for each column field
        result_list[math.floor(i/10000)].append(tuple_(*values))            # append namedtuple to inner list
    result = [k for m in result_list for k in m]                            # combine all inner lists into a single list
    print("Cardinality of project: ", len(result))
    return set(result)

def select(relation, predicate):
    iterations = math.ceil(len(relation) / 10000)                           # calc no of inner lists required
    result_list = []                                                        # initialize result list
    for i in range(iterations):
        start = i * 10000                                                   # calc starting index
        end = (i + 1) * 10000                                               # calc end index
        rel_batch_list = list(relation)[start:end]
        result_list.append([j for j in rel_batch_list if predicate(j)])     # append to result_list if predicate
    result = [k for m in result_list for k in m]                            # combine inner lists to a single list
    print("Cardinality of select: ", len(result))
    return set(result)


def rename(relation, new_columns=None, new_relation=None):
    # assuming new_relation argument is a string
    all_columns = list(next(iter(relation))._asdict().keys())               # get all column names
    if new_relation == None:
        tuple_name = next(iter(relation)).__repr__().split("(")[0]
    else:
        tuple_name = new_relation
    tuple_ = collections.namedtuple(tuple_name, new_columns)                # create namedtuple
    iter_ = iter(relation)                                                  # iterator for input set
    iterations = math.ceil(len(relation)/10000)                             # calc no of inner lists
    result_list = [[] for i in range(iterations)]                           # initialize result_list
    for i in range(len(relation)):
        record = next(iter_)
        values = [record._asdict()[j] for j in all_columns]                 # get values for all column field
        result_list[math.floor(i/10000)].append(tuple_(*values))            # append to inner list
    result = [k for m in result_list for k in m]                            # combine inner lists
    print("Cardinality of rename: ", len(result))
    return set(result)


def cross(relation1, relation2):
    rel1_columns = list(next(iter(relation1))._fields)                      # get relation1 columns
    rel2_columns = list(next(iter(relation2))._fields)                      # get relation2 columns
    rel1_name = next(iter(relation1)).__repr__().split("(")[0]              # get relation1 name
    rel2_name = next(iter(relation2)).__repr__().split("(")[0]              # get relation2 name
    new_rel1_columns = rel1_columns.copy()                                  # create copy to compare with original cols
    new_rel2_columns = rel2_columns.copy()
    # rename column names if similar named columns are found
    new_rel1_columns = [(rel1_name + "_" + new_rel1_columns[i]) if (new_rel1_columns[i] in rel2_columns) else new_rel1_columns[i] for i in range(len(new_rel1_columns))]
    new_rel2_columns = [(rel2_name + "_" + new_rel2_columns[i]) if (new_rel2_columns[i] in rel1_columns) else new_rel2_columns[i] for i in range(len(new_rel2_columns))]
    tuple_ = collections.namedtuple("Result", new_rel1_columns + new_rel2_columns)  # create a tuple with all columns
    iterations = math.ceil((len(relation1) * len(relation2)) / 10000)       # calc no of inner lists
    result_list = [[] for i in range(iterations)]                           # initialize result list
    iter1 = iter(relation1)                                                 # iterator for relation1
    for i in range(len(relation1)):
        record1 = next(iter1)
        values1 = list(record1.__getnewargs__())                            # get values for all columns in relation1
        iter2 = iter(relation2)                                             # iterator for relation2
        for j in range(len(relation2)):
            record2 = next(iter2)
            values2 = list(record2.__getnewargs__())                        # get values for all columns in relation2
            result_list[math.floor(i / 10000)].append(tuple_(*(values1 + values2))) # append to inner list
    result = [k for m in result_list for k in m]                            # combine inner lists to a single list
    print("Cardinality of cross: ", len(result))
    return set(result)


def theta_join(relation1, relation2, predicate):
    rel1_columns = list(next(iter(relation1))._fields)                      # get relation1 columns
    rel2_columns = list(next(iter(relation2))._fields)                      # get relation2 columns
    tuple_ = collections.namedtuple("Result", rel1_columns + rel2_columns)  # create tuple  with all columns
    iterations = math.ceil((len(relation1) * len(relation2)) / 10000)       # calc number of inner lists
    result_list = [[] for i in range(iterations)]                           # initialize result_list with inner lists
    iter1 = iter(relation1)                                                 # iterator for relation1
    for i in range(len(relation1)):
        record1 = next(iter1)
        iter2 = iter(relation2)                                             # iterator for relation2
        for j in range(len(relation2)):
            record2 = next(iter2)
            if predicate(record1, record2):
                resultdict = record1._asdict()                              # assign record1 to resultdict
                for k in rel2_columns:
                    resultdict[k] = record2._asdict()[k]                    # add cols and values from relation2
                result_list[math.floor(i / 10000)].append(tuple_(**resultdict))     # append to result_list
                break
    result = [k for m in result_list for k in m]                            # combine inner lists to new list
    print("Cardinality of theta join: ",len(result))
    return set(result)


def natural_join(relation1, relation2):
    rel1_columns = list(next(iter(relation1))._fields)                      # get relation1 columns
    rel2_columns = list(next(iter(relation2))._fields)                      # get relation2 columns
    common_col = None                                                       # variable to keep common column name
    for i in rel1_columns:
        if i in rel2_columns:
            common_col = i
    if (common_col == None):
        raise ValueError("No common Column found")                          # if no common column found throw error
    all_columns = list(set(rel1_columns + rel2_columns))
    tuple_ = collections.namedtuple("Result", all_columns)                  # create tuple with all columns
    iterations = math.ceil((len(relation1) * len(relation2)) / 10000)       # calc no of inner lists
    result_list = [[] for i in range(iterations)]                           # initialize result_list
    iter1 = iter(relation1)                                                 # iterator for relation1
    for i in range(len(relation1)):
        record1 = next(iter1)
        iter2 = iter(relation2)                                             # iterator for relation2
        for j in range(len(relation2)):
            record2 = next(iter2)
            if record1._asdict()[common_col] == record2._asdict()[common_col]:
                resultdict = record1._asdict()                              # assign record1 to resultdict
                for k in rel2_columns:
                    if k != common_col:
                        resultdict[k] = record2._asdict()[k]                # add cols and values from relation2
                result_list[math.floor(i / 10000)].append(tuple_(**resultdict))     # append to inner list
    result = [k for m in result_list for k in m]                            # combine inner lists to single list
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


#Query 5 from Project 1
print("\n\nQUERY 5 (Project 1 query 1)\n")
pprint.pprint(
    project(
        select(
            natural_join(
                Album, Artist
            ),
            lambda t: t.Name == 'Red Hot Chili Peppers'),
        ['Title']
    )
)


#Query 5 Optimized
print("\n\nOptimized QUERY 5 (Project 1 query 1)\n")
pprint.pprint(
    project(
        natural_join(
            Album,
            select(
                Artist, lambda t: t.Name == 'Red Hot Chili Peppers'
            )
        ),
        ['Title']
    )
)
