#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 27 11:23:23 2019

@author: dh
"""

import numpy as np
import functools

import importlib

import updawg.utils.bases as bases

importlib.reload(bases)

#%%
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


class NodeSet(set):
    def __init__(self, *args, **kwargs):
        for arg in args:
            self.add(arg)

    def add(self, val):
        if isinstance(val, Node):
            set.add(self, val)

    def union(self, other):
        for val in other:
            self.add(val)

    def __str__(self):
        return obj_set_to_str(self)

#%%

class Node(bases.PrettyReprBaseClass):
    _node_ctr = 0

    @classmethod
    def increment_node_ctr(cls):
        cls._node_ctr += 1

    def __str__(self):
        if self.label:
            return f'{self.label}'
        else:
            return f'{self.__class__.__name__} {self.node_num}'

    def print(self):
        PADDING = 10
        out_str_list = []

        print_str_dict = dict(parents=self.parents,
                              self=self,
                              children=self.children)

        for label, val in print_str_dict.items():
            if not str(val):
                continue

            this_line = f'{label:<{PADDING}s}{str(val)}'
            out_str_list.append(this_line)

        out_str = '\n'.join(out_str_list)
        print(out_str)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.node_num = self._node_ctr
        self.increment_node_ctr()

        label       = kwargs.get('label', f'node_{self.node_num}')
        parents     = kwargs.get('parents', [])
        children    = kwargs.get('children', [])

        self.label      = label
        self.parents    = NodeSet(*parents)
        self.children   = NodeSet(*children)

    def connect_to_object(self, obj, callback=None):
        '''
        Connect node to an object so that the specified object method
        gets called
        '''
        self.obj = obj

        if callback is None:
            return

        try:
            self.callback = obj.__getattribute__(callback)
        except (AttributeError, TypeError):
            return

        # Probably shouldn't do this, but... trying to shoehorn the
        # object's callback method name into this class's namespace
        if callback not in self.__dict__:
            self.__dict__[callback] = self.callback


#%%
class NodeMapping(bases.PrettyReprBaseClass):

    def __init__(self, node_mapping, **kwargs):
        bases.PrettyReprBaseClass.__init__(self, node_mapping, **kwargs)

        assert isinstance(node_mapping, dict)

        tmp_dict = {}

        for key, val in node_mapping.items():
            assert isinstance(key, Node)
            assert isinstance(val, NodeSet)

            tmp_dict[key] = val

        self._dict = tmp_dict

        self.num_nodes = 0
        self._all_nodes = NodeSet()
        self._node_indices = {}
        self.adj_matrix = np.zeros([0,0], dtype=int)

        self._accounting()

    def _accounting(self):
        self._count_all_nodes()
        self._map_nodes_to_indices()
        self._create_adjacency_matrix()

    def _count_all_nodes(self):
        all_nodes = NodeSet()

        for node, child_nodes in self._dict.items():
            all_nodes.add(node)
            for child_node in child_nodes:
                all_nodes.add(child_node)

        self._all_nodes = all_nodes
        self.num_nodes = len(all_nodes)

    def _map_nodes_to_indices(self):
        for idx, node in enumerate(self._all_nodes):
            self._node_indices[node] = idx

    def _create_adjacency_matrix(self):
        n = self.num_nodes
        A = np.zeros([n,n], dtype=int)

        for node, child_nodes in self._dict.items():
            i = self._node_indices[node]

            for child_node in child_nodes:
                j = self._node_indices[child_node]

                A[i,j] += 1

        self.adj_matrix = A


    def find_cycles(self):
        '''
        Taking powers of adjacency matrix gives number of paths of given length
        between nodes. Taking geometric series gives total number of paths.
        If resulting sum has non-zeros on diagonal, then there are cycles.
        '''

        n = self.num_nodes
        A = self.adj_matrix
        P = np.eye(n, dtype=int)
        T = np.zeros(A.shape, dtype=int)

        for _ in range(n):
            P = P @ A
            T += P

        diag = T.diagonal()

        if not any(diag):
            return None

        idx_cycle = np.argwhere(diag > 0).flatten()
        ind = self._node_indices

        cycle_nodes = [node for node, idx in ind.items() if idx in idx_cycle]
        return cycle_nodes




#%%
def tmp():
    #%%
    A = Node(label='parent A')
    B = Node(label='parent B')

    a = Node(label='node a')
    b = Node(label='node b')
    c = Node(label='node c')

#    node_mapping = {A:NodeSet(a,b),
#                    B:NodeSet(b,c),
#                    a:NodeSet(b),
#                    b:NodeSet(c),
#                    c:NodeSet(A)}

    node_mapping = {A:NodeSet(a,b),
                    B:NodeSet(b,c),
                    b:NodeSet(c)}


    self = DiGraph(node_mapping)

#    self = NodeMapping(node_mapping)



#%%


class DiGraph(bases.PrettyReprBaseClass):

    def __init__(self, node_mapping=None, **kwargs):
        self.update_node_mapping(node_mapping)

    def update_node_mapping(self, node_mapping, **kwargs):
        self.node_mapping = NodeMapping(node_mapping)


    def add_node(self, node=None, parents=None, children=None, **kwargs):
        node_mapping = self.node_mapping

        parent_nodes = NodeSet(parents)
        child_nodes  = NodeSet(children)

        self.add_children(node=node,
                          children=child_nodes,
                          update_node_mapping=False)

        for parent in parent_nodes:
            self.add_children(node=parent,
                              children=[node],
                              update_node_mapping=False)

        self.update_node_mapping(node_mapping)

    def add_parents(self, node=None, parents=None, **kwargs):
        node_mapping = self.node_mapping

        for parent in parents:
            self.add_children(node=parent,
                              children=node,
                              update_node_mapping=False)

        self.update_node_mapping(node_mapping)


    def remove_parents(self, node=None, parents=None, **kwargs):
        node_mapping = self.node_mapping

        for parent in parents:
            self.remove_children(node=parent,
                                 children=[node],
                                 update_node_mapping=False)

        self.update_node_mapping(node_mapping)

    def add_children(self, node=None, children=None, update_node_mapping=True, **kwargs):
        node_mapping = self.node_mapping

        if node not in node_mapping:
            node_mapping[node] = children
        else:
            child_nodes = node_mapping[node]

            for child_node in child_nodes:
                child_nodes.add(children)

        if update_node_mapping:
            self.update_node_mapping(node_mapping)

    def remove_children(self, node=None, children=None, update_node_mapping=True, **kwargs):
        node_mapping = self.node_mapping

        child_nodes = node_mapping[node]

        for this_child in children:
            if this_child in child_nodes:
                child_nodes.remove(this_child)

        if update_node_mapping:
            self.update_node_mapping(node_mapping)


    def has_cycles(self):
        cycles = self.node_mapping.find_cycles()

        return len(cycles) > 0


    # TODO: printing functions (to be able to view the graph)



#class DAG(DiGraph):
#
#    def __init__(self, *args, **kwargs):
#        super().__init__(*args, **kwargs)
#
#        self.



#%%


'''
Usage:

extractor   = MyDataExtractor()
processor   = MyDataProcessor()
handler     = MyDataHandler()

TODO: make components hashable

#### dict of parent:{set/list of child nodes} pairs
node_mapping = {extractor:[processor],
                processor:[handler]}

my_pipeline = DAG(node_mapping=node_mapping)

my_pipeline.print()

input_data_dict = dict(file=/path/to/file, ...)
output_data_dict = dict(...)

my_pipeline.set_inputs(input_data_dict)
my_pipeline.set_outputs(output_data_dict)

my_pipeline.run()





OR

something like


extractor_A = DataExtractor_A()
extractor_B = DataExtractor_B()

processor_A = DataProcessor_A()
processor_B = DataProcessor_B()
processor_C = DataProcessor_C()
processor_D = DataProcessor_D()
...
processor_N = DataProcessor_N()

processors = [processor_A, ..., processor_N]

handler     = MyDataHandler()

node_mapping = {extractor_A:processors,
                extractor_B:processors,
                processor_A:[handler],
                processor_B:[handler],
                ...
                processor_N:[handler]}

data_dag = create_dag(node_mapping=node_mapping)


input_data_dict = dict(...)
output_data_dict = dict(...)

data_dag.set_inputs(input_data)
data_dag.set_outputs(output_data)

data_dag.run()





'''

