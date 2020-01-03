#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 24 22:49:44 2019

@author: dh



The goal is to have an easy-to-use framework that forces tool developers to
design with modularity in mind, and ultimately lead to more component re-use.

For example, an existing plotting tool might have significant overlap with a
new ready-to-be-written tool for aggregating metrics, but the existing tool
could be written in such a way that code re-use would be more work than writing
something from scratch.

Suppose instead that the plotting tool had the following design:
            Extractor   ->  Processor   ->  PlotHandler
Then the metric-aggregating tool could reuse the existing Extractor and
Processor component, and only need a new MetricAggregator component. The new
tool would follow this design:
            Extractor   ->  Processor   ->  MetricAggregator



This started as a framework for data analysis tools, which tend to follow (or
have components that follow) certain patterns.

One pattern that I see over and over is what I've been calling (for lack of a
better term) a Pipeline:
                Extractor   ->  Processor   ->  Handler

The Extractor reads data from a set of files, the Processor does a set of
computations that are specific to the use case, and the Handler does something
with the processed data: plots the data, writes the data to a text file, caches
the data for use by another process, etc.

Another pattern is what I've been calling a Manager. It is a container for
multiple similar components that can be hooked up in series or parallel.

These patterns can each be described as directed acyclic graphs (DAGs), whose
nodes are the individual components. This suggests having a DAG framework on
top of which to define some atomic parts  -- for lack of better terms, I'm
thinking of these as DataComponent objects that would act on DataCache
dictionary-like data structures (dicts in memory, or JSON/HDF5 files).

The functionality of the DataComponent objects would be done by subclassing.
For example, DataExtractorBase, DataProcessorBase, and DataHandlerBase could
all be subclasses of DataComponent, which add (resp.) .extract, .process, and
.handle methods, and then MyDataExtractor would override the .extract method
(and similarly for MyDataProcessor and MyDataHandler).




"""



# TODO: connect subcomponents, so that outputs go to the correct inputs
# TODO: register subcomponents to a parent, so that parent's .run method
#       calls children's .run methods in correct order

import abc
import copy

import updawg.utils.common as common

#%%

# TODO: when the object is modified, show the args for the updates

class PrettyReprBaseClass:
    '''
    Implements __repr__ to return "MyClass(*args, **kwargs)"
    '''
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = copy.deepcopy(kwargs)

    def __repr__(self):
        parg_list = [str(arg) for arg in self._args]

        kwarg_list = [f'{key}={str(val)}' for key, val in self._kwargs.items()]
        arg_list = parg_list + kwarg_list
        arg_str = ", ".join(arg_list)

        class_name = self.__class__.__name__
        repr_str = f'{class_name}({arg_str})'
        return repr_str



#%%
class DataComponent(PrettyReprBaseClass):
    '''
    Base class for other data analysis classes
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.inputs = {}
        self.outputs = {}

        self.configure(*args, **kwargs)

    def configure(self, *args, **kwargs):
        pass

#    def __str__(self):
#        pass
        # f'{self.__name__} of class {self.__class__.__name__}
        #       .subcomponent_1 : {self.subcomponent_1.__name__}
        #       ...
        #       .subcomponent_N : {self.subcomponent_N.__name__}
        #
        # TODO: figure out how to do this dynamically

    def run(self, *args, **kwargs):
        pass

#%%
class DataComponentList(list):
    def __init__(self, *args, **kwargs):
        for arg in args:
            self.append(arg)

    def add(self, val):
        if isinstance(val, DataComponent):
            list.append(self, val)

    def extend(self, iterable):
        other = [x for x in iterable if isinstance(x, DataComponent)]

        list.extend(self, other)

    def insert(self, index, obj):
        if isinstance(obj, DataComponent):
            list.insert(self, index, obj)

    def __str__(self):
        return common.obj_list_to_str(self)

#%%
class DataExtractorBase(DataComponent):
    '''
    Extract raw data into a format the processor can use
    '''
    @property
    def run(self):
        return self.extract_data

    def configure(self, *args, **kwargs):
        ''' Define this in the subclass '''
        pass

    def extract_data(self, *args, **kwargs):
        ''' Define this in the subclass '''
        print('extracting data')

#%%
class DataProcessorBase(DataComponent):
    '''
    Process the data
    '''
    @property
    def run(self):
        return self.process_data

    def configure(self, *args, **kwargs):
        ''' Define this in the subclass '''
        pass

    def process_data(self, *args, **kwargs):
        ''' Define this in the subclass '''
        print('processing data')

#%%
class DataHandlerBase(DataComponent):
    '''
    Do something with the processed data
    '''
    def configure(self, *args, **kwargs):
        ''' Define this in the subclass '''
        pass

    @property
    def run(self):
        return self.handle_data

    def handle_data(self, *args, **kwargs):
        ''' Define this in the subclass '''
        print('handling data')


