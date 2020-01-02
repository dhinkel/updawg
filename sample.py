#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  2 13:08:03 2020

@author: dh
"""

import pandas as pd
import matplotlib.pyplot as plt

import updawg.components as components

#%%

class MyExtractor(components.DataExtractorBase):

    def configure(self, *args, **kwargs):
        self.header_row = 15

    def extract(self, *args, **kwargs):
        file_name = kwargs.pop('file_name', None)

        file_data = pd.read_csv(file_name, header=self.header_row)

        self.outputs['raw'] = file_data

class MyProcessor(components.DataProcessorBase):

    def configure(self, *args, **kwargs):
        pass

    def process(self, *args, **kwargs):
        inputs = self.inputs

        df = inputs['df']

        # do the processing

        self.outputs['df'] = df

class MyHandler(components.DataHandlerBase):

    def configure(self, *args, **kwargs):
        pass

    def handle(self, *args, **kwargs):
        self.plot_something(*args, **kwargs)
        self.plot_something_else(*args, **kwargs)
        self.plot_another_something(*args, **kwargs)

    def plot_something(self, *args, **kwargs):
        data = self.inputs['df']

        x_data = data['x']
        y_data = data['y']

        fig, ax = plt.subplots(111)

        ax.plot(x_data, y_data)





class MyPipeline(components.DataPipelineBase):

    def configure(self, *args, **kwargs):
        self.extractor  = MyExtractor()
        self.processor  = MyProcessor()
        self.handler    = MyHandler()



#%%

def run_pipeline(file_name=None):

    inputs = dict(file_name=file_name)
    output_kwargs = dict(save_figs=False)

    pipeline = MyPipeline(**inputs)

    pipeline.run(**output_kwargs)



def main():
    file_name = '/path/to/file'

    run_pipeline(file_name=file_name)


if __name__ == '__main__':
    main()
