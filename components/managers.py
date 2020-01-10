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

class DataPipeline(DataManagerBase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print(args)
        for arg in args:
            if isinstance(arg, bases.DataComponent):
                self.components.append(arg)

        self.connect_components()

    def connect_components(self, *args, **kwargs):
        comp = self.components
        for a, b in zip(comp, comp[1:]):
            b._inputs.point_to(a._outputs)

        self._inputs.point_to(self.components[0]._inputs)
        self._outputs.point_to(self.components[-1]._outputs)

    def run(self, *args, **kwargs):
        '''
        Assumes that component.run sets its .outputs at the end
        '''
        for component in self.components:
            component.run(*args, **kwargs)




#class DataPipeline(DataManagerSerial):
#
#    components = bases.DataComponentList()
#
#    def __init__(self, *args, **kwargs):
#        super().__init__(*args, **kwargs)
#
#        cls = bases.DataComponent
#        self.components = bases.DataComponentList(cls(), cls(), cls())
#
#    @property
#    def extractor(self):
#        return self.components[0]
#
#    @extractor.setter
#    def extractor(self, val):
#        assert isinstance(val, bases.DataComponent)
#        self.components[0] = val
#
#    @property
#    def processor(self):
#        return self.components[1]
#
#    @processor.setter
#    def processor(self, val):
#        assert isinstance(val, bases.DataComponent)
#        self.components[1] = val
#
#    @property
#    def handler(self):
#        return self.components[2]
#
#    @handler.setter
#    def handler(self, val):
#        assert isinstance(val, bases.DataComponent)
#        self.components[2] = val

#
#class DataPipeline(DataManagerSerial):
#
#    components = bases.DataComponentList()
#
#    def __init__(self, *args, **kwargs):
#        super().__init__(*args, **kwargs)
#
#        cls = bases.DataComponent
#        self.components = bases.DataComponentList(cls(), cls(), cls())
#
#    @property
#    def extractor(self):
#        return self.components[0]
#
#    @extractor.setter
#    def extractor(self, val):
#        assert isinstance(val, bases.DataExtractorBase)
#        self.components[0] = val
#
#    @property
#    def processor(self):
#        return self.components[1]
#
#    @processor.setter
#    def processor(self, val):
#        assert isinstance(val, bases.DataProcessorBase)
#        self.components[1] = val
#
#    @property
#    def handler(self):
#        return self.components[2]
#
#    @handler.setter
#    def handler(self, val):
#        assert isinstance(val, bases.DataHandlerBase)
#        self.components[2] = val