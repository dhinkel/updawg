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
        super().__init__(*args, **kwargs)
        self.components = bases.DataComponentList()


class DataManagerParallel(DataManagerBase):

    def run(self, *args, **kwargs):
        funcs_for_loop = [cmpt.run for cmpt in self.components]

        outputs = map_parallel(inputs=self._inputs,
                               functions=funcs_for_loop)

        self.outputs = outputs

class DataManagerSerial(DataManagerBase):

    def run(self, *args, **kwargs):
        next_input = self.inputs
        for component in self.components:
            component.inputs = next_input
            component.run(*args, **kwargs)
            next_input = component.outputs

        self.outputs = next_input


_cls = bases.DataComponent

class DataPipeline(DataManagerSerial):

    components = bases.DataComponentList()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        cls = bases.DataComponent
        self.components = bases.DataComponentList(cls(), cls(), cls())

    @property
    def extractor(self):
        return self.components[0]

    @extractor.setter
    def extractor(self, val):
        assert isinstance(val, bases.DataExtractorBase)
        self.components[0] = val

    @property
    def processor(self):
        return self.components[1]

    @processor.setter
    def processor(self, val):
        assert isinstance(val, bases.DataProcessorBase)
        self.components[1] = val

    @property
    def handler(self):
        return self.components[2]

    @handler.setter
    def handler(self, val):
        assert isinstance(val, bases.DataHandlerBase)
        self.components[2] = val