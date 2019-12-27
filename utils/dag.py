#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 27 11:23:23 2019

@author: dh
"""

import copy

import importlib

import updawg.utils.bases as bases

importlib.reload(bases)

#%%
def obj_list_to_str(obj_list):
    '''
    Print list to use obj.__str__ instead of obj.__repr__ for each obj
    '''
    out_str_list = []
    for obj in obj_list:
        out_str_list.append(str(obj))

    if out_str_list:
        aux_str = ", ".join(out_str_list)
        out_str = f'[{aux_str}]'
    else:
        out_str = ''

    return out_str


#%%

class Node(bases.PrettyReprBaseClass):
    _node_ctr = 0

    @classmethod
    def increment_node_ctr(cls):
        cls._node_ctr += 1

#    def __str__(self):
#        out_str_list = []
#        str_dict = dict(parents=self.parents,
#                        self=self,
#                        children=self.children)
#
#        out_str_list = [f'{key:<10s}:{str(val)}' for key,val in str_dict.items() if str(val)]
#
#        out_str = "\n".join(out_str_list)
#
#        return out_str


    def __str__(self):
        return f'{self.__class__.__name__} {self.node_num}'

    def print(self):
        out_str_list = []

        print_str_dict = dict(parents=self.parents,
                              self=self,
                              children=self.children)

        for label, val in print_str_dict.items():
            if not str(val):
                continue

            this_line = f'{label:<10s}{str(val)}'
            out_str_list.append(this_line)

        out_str = '\n'.join(out_str_list)
        print(out_str)


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.node_num = self._node_ctr
        self.increment_node_ctr()

        label       = kwargs.get('label', None)
        parents     = kwargs.get('parents', list())
        children    = kwargs.get('children', list())

        if label is None:
            label = f'node_{self.node_num}'

        self.label      = label

        parent_nodes = []
        for parent in parents:
            if isinstance(parent, self.__class__):
                parent_nodes.append(parent)

        child_nodes = []
        for child in children:
            if isinstance(child, self.__class__):
                child_nodes.append(child)

        self.parents = NodeList(*parent_nodes)
        self.children = NodeList(*child_nodes)


#%%
class NodeList(bases.PrettyReprBaseClass, list):

    def __init__(self, *args, **kwargs):
        bases.PrettyReprBaseClass.__init__(self, *args, **kwargs)
        self._list = []
        for arg in args:
            if isinstance(arg, Node):
                self._list.append(arg)

    def __str__(self):
        return obj_list_to_str(self._list)

#%%

def tmp():
    #%%

    self = Node()

            #%%

class tmp:

    def create_parent(self, *args, label=None, **kwargs):
        parent_node = self.__class__(*args, label=label, child=self, **kwargs)
        self.add_parent(parent_node)

    def create_child(self, *args, label=None, **kwargs):
        child_node = self.__class__(*args, label=label, parent=self, **kwargs)
        self.add_child(child_node)

    def add_parent(self, parent_node):
        assert isinstance(parent_node, self.__class__)

    def add_child(self, child_node):
        assert isinstance(child_node, self.__class__)