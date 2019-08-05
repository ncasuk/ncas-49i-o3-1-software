import os
import stat
import pathlib
import inspect
import multiprocessing
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import time
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import bokeh
import pandas_bokeh
def get_df_list(flist):
    df_list = []
    multi = True
    if multi:
        mypool = multiprocessing.Pool(10)
        result = mypool.map_async(read_file,flist,chunksize = 1)   
        mypool.close()
        previous_number = result._number_left
        while True:
            if (result.ready()): break
            remaining = result._number_left
            if remaining != previous_number:
                print ("Waiting for", remaining, "files to be read...")
                previous_number = remaining
            time.sleep(5)
        df_list = result.get()       
    else:       
        for i in flist:
            df_list.append(read_file(i))
    return df_list
        
def read_file(fname):
    try:
        df = pd.read_csv(fname,index_col = 0,parse_dates = True)
    except Exception as e:
        df = str(fname) + "\t" + str(e)
    return df

def split_df_list(df_list):
    successes = [i for i in df_list if isinstance(i,pd.DataFrame)]
    fails = [i for i in df_list if not isinstance(i,pd.DataFrame)]
    return (fails,successes)

def find_local_files(path,exts):
    file_list = []
    for root, dirs, files in os.walk(path):
        for currentFile in files:
            if any(currentFile.lower().endswith(ext) for ext in exts):
                file_list.append(os.path.join(root, currentFile))
    return file_list

def tidy_df(df):
    df.loc[df["Primary Instrument Flags"] == 3,"Primary Instrument"] = np.nan
    df.loc[df["Primary Instrument Flags"] == 5,"Primary Instrument"] = np.nan
    df.loc[df["Secondary Instrument Flags"] == 3,"Secondary Instrument"] = np.nan
    df.loc[df["Secondary Instrument Flags"] == 5,"Secondary Instrument"] = np.nan
    return df

def plot_both(df):
    fig = go.Figure([go.Scatter(x=df["date"], y=df["Primary Instrument"])])
    #fig = px.line(df,x = "date",y = "Primary Instrument")
    #plt.scatter(df["Primary Instrument"].dropna().index.values,df["Primary Instrument"].dropna())
    #plt.scatter(df["Secondary Instrument"].dropna().index.values,df["Secondary Instrument"].dropna())
    #df["Primary Instrument"].dropna().plot()
    #df["Secondary Instrument"].dropna().plot()
    #plt.show()
    fig.show(renderer = "png")
    #df["Primary Instrument"].dropna().plot_bokeh()
    #plt.show()
def plot_diff(df):
    #df["diff"] = (abs(df["Primary Instrument"] - df["Secondary Instrument"])/((df["Primary Instrument"] + df["Secondary Instrument"])/2))*100
    #plt.scatter(df["diff"].dropna().index.values,df["diff"].dropna())
    #df["diff"].dropna().plot()
    #plt.show()
    return

def run_me(year = None):
    if year == None:
        year = int(datetime.datetime.now().year)
    curr_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    conf_dir = os.path.join(curr_dir,"config")
    parent_dir = str(pathlib.Path(curr_dir).parent)
    data_dir = os.path.join(parent_dir,"Input_data")
    output_dir = os.path.join(parent_dir,"Output")
    
    flist = find_local_files(output_dir,[".csv"])
    df_list = get_df_list(flist)
    error_list,df_list = split_df_list(df_list)
    giant_df = pd.concat(df_list,sort = True).sort_index()
    giant_df = tidy_df(giant_df)
    giant_df["date"] = giant_df.index.values
    
    plot_both(giant_df)
    plot_diff(giant_df)
    #plot_roll_ave(giant_df)
    #plot_roll_diff(giant_df)
    #plot_one_and_two(giant_df)
    
    if len(error_list) > 0:
        print(error_list)
    return giant_df

if __name__ == "__main__":
    df = run_me()
    
