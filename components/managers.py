#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  2 19:07:47 2020

@author: dh
"""


import updawg.components.bases as bases
from updawg.utils import map_parallel

#%%

class DataManagerBase(bases.DataComponent):

    def __init__(self, *args, **kwargs):
        self.is_parallel = kwargs.pop('is_parallel', False)

        super().__init__(*args, **kwargs)
        self.components = bases.ComponentList()


class DataManagerParallel(DataManagerBase):

    def run(self, *args, **kwargs):
        funcs_for_loop = [cmpt.run for cmpt in self.components]

        outputs = map_parallel(inputs=self._inputs,
                               functions=funcs_for_loop)

        self._outputs = outputs

class DataManagerSerial(DataManagerBase):

    def run(self, *args, **kwargs):
        this_input = self._inputs
        for component in self.components:
            component._inputs = this_input
            this_output = component.run(this_input)
            this_input = this_output

        self._outputs = this_output




class DataPipeline(DataManagerSerial):

    @property
    def extractor(self):
        return self._components[0]

    @extractor.setter
    def extractor(self, val):
        assert isinstance(val, bases.DataExtractorBase)
        self._components[0] = val

    @property
    def processor(self):
        return self._components[1]

    @processor.setter
    def processor(self, val):
        assert isinstance(val, bases.DataProcessorBase)
        self._components[1] = val

    @property
    def handler(self):
        return self._components[2]

    @handler.setter
    def handler(self, val):
        assert isinstance(val, bases.DataHandlerBase)
        self._components[2] = val