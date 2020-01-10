#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  2 22:54:03 2020

@author: dh
"""

import functools

def obj_iter_to_str(obj_list, iter_type=list):
    '''
    Print list/set to use obj.__str__ instead of obj.__repr__ for each obj
    '''
    if iter_type == list:
        lchar = '['
        rchar = ']'
    if iter_type == set:
        lchar = '{'
        rchar = '}'

    out_str_list = []
    for obj in obj_list:
        out_str_list.append(str(obj))

    if out_str_list:
        aux_str = ", ".join(out_str_list)
        out_str = f'{lchar}{aux_str}{rchar}'
    else:
        out_str = ''

    return out_str

obj_list_to_str = functools.partial(obj_iter_to_str, iter_type=list)
obj_set_to_str = functools.partial(obj_iter_to_str, iter_type=set)



def is_hashable(obj):
    try:
        hash(obj)
    except TypeError:
        output = False
    else:
        output = True

    return output