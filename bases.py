#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 24 22:49:44 2019

@author: dh
"""

import abc


#%%

class DataObject_ABC(abc.ABC):
    '''
    Abstract base class for other data analysis classes
    '''
    def __init__(self, *args, **kwargs):
        self.configure(*args, **kwargs)

    def configure(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def run(self, *args, **kwargs):
        pass

#%%
class DataExtractorBase(DataObject_ABC):
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
class DataProcessorBase(DataObject_ABC):
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
class DataHandlerBase(DataObject_ABC):
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
class DataPipelineBase(DataObject_ABC):
    '''
    Represents a common data analysis pattern:
            Extractor -> Processor -> Handler
    '''
    def configure(self, *args, **kwargs):
        ''' Define this in the subclass '''
        self.extractor  = DataExtractorBase()
        self.processor  = DataProcessorBase()
        self.handler    = DataHandlerBase()

    def extract_data(self, *args, **kwargs):
        self.extractor.extract_data(*args, **kwargs)

    def process_data(self, *args, **kwargs):
        self.processor.process_data(*args, **kwargs)

    def handle_data(self, *args, **kwargs):
        self.handler.handle_data(*args, **kwargs)

    def run(self, *args, **kwargs):
        self.extractor.run(*args, **kwargs)
        self.processor.run(*args, **kwargs)
        self.handler.run(*args, **kwargs)

#%%
class DataManagerBase(DataObject_ABC):
    '''
    Contains DataPipeline objects
    '''
    def configure(self, *args, **kwargs):
        ''' Define this in the subclass '''
        self.pipelines = {}
        self.pipelines['1st'] = DataPipelineBase(*args, **kwargs)
        self.pipelines['2nd'] = DataPipelineBase(*args, **kwargs)
        self.pipelines['3rd'] = DataPipelineBase(*args, **kwargs)

    def extract_data(self, *args, **kwargs):
        for pipeline in self.pipelines.values():
            pipeline.extract_data(*args, **kwargs)

    def process_data(self, *args, **kwargs):
        for pipeline in self.pipelines.values():
            pipeline.process_data(*args, **kwargs)

    def handle_data(self, *args, **kwargs):
        for pipeline in self.pipelines.values():
            pipeline.handle_data(*args, **kwargs)

    def run(self, *args, **kwargs):
        for pipeline in self.pipelines.values():
            pipeline.run(*args, **kwargs)


