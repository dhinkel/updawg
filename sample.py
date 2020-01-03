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


class MyDataExtractor(components.DataExtractorBase):

    def extract_data(self, *args, **kwargs):
        print(args)
        print(kwargs)
        file_in = kwargs.pop('file_in', None)
        print(f'file_in = {file_in}')

        file_data = pd.read_json(file_in)

        self.outputs = file_data

#%%
class MyDataProcessor(components.DataProcessorBase):

    def process_data(self, *args, **kwargs):
        df = self.inputs

        df_out = pd.DataFrame(df)

        # do the processing
        alt_km  = df['altitude']

        df_out['alt_m'] = alt_km * 1000

        self.outputs = df_out

#%%
class MyDataHandler(components.DataHandlerBase):

    def configure(self, *args, **kwargs):
        self.label_dict = dict(time='Time (s)',
                               alt_m='Altitude (m)'
                               )

    def handle_data(self, *args, **kwargs):
        self.plot_something(*args, **kwargs)

    def plot_something(self, *args, **kwargs):
        file_out = kwargs.pop('file_out', '')

        data = self.inputs

        x_var = 'time'
        y_var = 'alt_m'

        x_data = data[x_var]
        y_data = data[y_var]

        x_label = self.label_dict[x_var]
        y_label = self.label_dict[y_var]

        fig, ax = plt.subplots(1,1)

        ax.plot(x_data, y_data)

        ax.set_xlabel(x_label)
        ax.set_ylabel(y_label)

        title = f'{x_label} vs {y_label}'
        ax.set_title(title)

        fig.tight_layout()

        if file_out:
            self.save_fig(fig, file_out, **kwargs)

    def save_fig(self, fig, file_out, **kwargs):
        fig.savefig(file_out, **kwargs)



class MyDataPipeline(components.DataPipeline):

    def configure(self, *args, **kwargs):
        self.extractor  = MyDataExtractor()
        self.processor  = MyDataProcessor()
        self.handler    = MyDataHandler()






#%%

def run_pipeline(file_name=None):

    kwargs = dict(file_in=file_name,
                  file_out='')

    print(kwargs)

    pipeline = MyDataPipeline()

#    pipeline.run(**kwargs)



def main():
    file_name = '/mnt/d/Repos/Telemetry-Data/TESS/JSON/analysed.json'
    return

    run_pipeline(file_name=file_name)


if __name__ == '__main__':
    main()
