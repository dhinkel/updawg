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


# TODO:
# This works for letting multiple components' inputs point
# to a single component's output, but doesn't work for letting
# a single input get fed from multiple outputs. That case seems
# like a dictionary would work better
class DataReference():
    '''
    Mutable data container that can be used as a pointer
    '''
    @property
    def value(self):
        return self._value[0]

    @value.setter
    def value(self, value):
        self._value[0] = value

    def __init__(self, val=None, **kwargs):
        self._value = [val]

    def __repr__(self):
        cls = self.__class__.__name__
        val = self.value
        return f'{cls}({repr(val)})'

    def point_to(self, other):
        assert isinstance(other, self.__class__)

        self._value = other._value

#%%
class DataComponent(PrettyReprBaseClass):
    '''
    Base class for other data analysis classes
    '''
    @property
    def inputs(self):
        return self._inputs.value

    @inputs.setter
    def inputs(self, value):
        self._inputs.value = value

    @property
    def outputs(self):
        return self._outputs.value

    @outputs.setter
    def outputs(self, value):
        self._outputs.value = value


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._inputs = DataReference()
        self._outputs = DataReference()

        self.configure(*args, **kwargs)

    def configure(self, *args, **kwargs):
        pass

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

#%%
def main():
    #%%
    x = DataReference(1)
    print(x, x.value, x._value, id(x), id(x._value))

    y = DataReference(2)
    print(y, y.value, y._value, id(y), id(y._value))

    y.point_to(x)
    print(y, y.value, y._value, id(y), id(y._value))




