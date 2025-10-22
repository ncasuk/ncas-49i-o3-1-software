import os
import stat
import pathlib
import inspect
import multiprocessing
import glob
import pandas as pd
import datetime
import xlrd
import calendar
import numpy as np
import time

def read_conf_file(fname):
    df = pd.read_csv(fname,delimiter = "\t")
    date_cols = [i for i in df.columns.values if "date" in i.lower()]
    for i in date_cols:
        df[i] = pd.to_datetime(df[i])
    return df

def read_conf_file_list(fname,splitter = None,item_number = None):
    with open(fname) as infile:
        return_list = [i.strip() for i in infile.readlines()]
    if splitter !=None:
        return_list = [i.split(splitter)[item_number] for i in return_list]
    return return_list

def get_df_list(flist,col_name,rename_col):
    df_list = []
    multi = False
    if multi:
        mypool = multiprocessing.Pool(10)
        result = mypool.map_async(read_file,[(i,col_name,rename_col) for i in flist],chunksize = 1)   
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
            df_list.append(read_file((i,col_name,rename_col)))
    
    return df_list
        
def read_file(args):
    fname,col_name,rename_col = args
    try:
        if col_name == "Ozone":
            df = pd.read_csv(fname,parse_dates = True, index_col = 0).loc[df[df["Primary Instrument Flags"]==1].index.values,col_name].rename(rename_col)
        else:
            df = pd.read_csv(fname,parse_dates = True, index_col = 0)[col_name].rename(rename_col)
    except Exception as e:
        df = str(fname) + "\t" + str(e)
        print(fname,"failed",str(e))
    return df

def get_date(day_val,time_val):
    return(np.datetime64(day_val + "T" + time_val))

def output_df(df,output_dir,year):
    create_folder([output_dir,year])
    for i in pd.unique(df[df.index.year == year].index.month):
        create_folder([output_dir,year,calendar.month_name[i]])
        for x in pd.unique(df[((df.index.year == year) & (df.index.month == i))].index.day):
            output_day(df,output_dir,year,i,x)

def create_folder(path_parts):
    os.makedirs(os.path.join(*list(map(str,path_parts))),exist_ok = True)

#def output_month(df,output_dir,year,month):
#    df[((df.index.year == year) & (df.index.month == month))].to_csv(os.path.join(*list(map(str,[output_dir,year,calendar.month_name[month],calendar.month_name[month]]))) + ".csv")

def output_day(df,output_dir,year,month,day):
    save_path = os.path.join(*list(map(str,[output_dir,year,calendar.month_name[month],"CVO_"+ str(year) + ("0" + str(month))[-2:] + ("0" + str(day))[-2:] + ".dat"])))
    write_header(save_path)
    df_output = get_output_df(df,year,month,day)
    df_output.to_csv(save_path,mode = "a",index = False,columns = ["date","hour","CO","Ozone","SO2","NO","NO2"],header = False,sep = "\t")

def get_output_df(df,year,month,day):
    df_output = df[((df.index.year == year) & (df.index.month == month) & (df.index.day ==day))].copy()
    df_output.reindex(pd.date_range(start = datetime.date(year,month,day),periods = 24,freq = "1H"))
    df_output.index = pd.to_datetime(df_output.index)
    df_output.loc[:,"SO2"] = -999.99
    df_output.loc[:,"NO"] = -999.99
    df_output.loc[:,"NO2"] = -999.99
    df_output = df_output.fillna(-999.99)
    df_output = df_output.round(2).applymap('{:.2f}'.format)
    df_output.loc[:,"date"] = df_output.index.strftime("%d.%m.%Y")
    df_output.loc[:,"hour"] = df_output.index.strftime("%H:%M:%S")
    return df_output

def write_header(save_path):
    header_rows = ["Cape Verde\t\t\t\t\t\t","\t16.85\t24.87\t10\t\t\t","Mixing ratios in ppbV;\t\t\t\tmissing\tvalue:\t-999.99","time[UTC+0h]\t\tCO\tO3\tSO2\tNO2\tNO"]
    with open(save_path,mode = "w") as write_file:
            for item in header_rows:
                write_file.write(item + "\n")

def find_local_files(path,exts):
    file_list = []
    for root, dirs, files in os.walk(path):
        for currentFile in files:
            if any(currentFile.lower().endswith(ext) for ext in exts):
                file_list.append(os.path.join(root, currentFile))
    return file_list



def output_errors(fpath,errors):
    try:
        with open(fpath) as err_file:
            err_list = [i.split("\t")[0] for i in err_file.readlines()]
    except:
        err_list = []
    errs = [i for i in errors if i.split("\t")[0] not in err_list]
    if len(errs):
        with open(fpath,"a") as err_file:
            for error in errs:
                err_file.write(error)

def filter_flist(flist):
    flist = [i for i in flist if "co_cal" not in i.lower()]
    return flist

def run_me(year = None):
    if year == None:
        year = int(datetime.datetime.now().year)
    curr_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    conf_dir = os.path.join(curr_dir,"config")
    #parent_dir = str(pathlib.Path(curr_dir).parent)
    #double_parent = str(pathlib.Path(parent_dir).parent)
    output_dir = "/gws/pw/j07/ncas_obs_vol1/cvao/processing/ncas-49i-o3-1/GAW_NRT"
    dirs_and_cols = read_conf_file_list(os.path.join(conf_dir,"dirs_and_cols.mww"))
    end_dfs = []
    for data_dir,data_col,rename_col in [i.split("\t") for i in dirs_and_cols]:
        flist = find_local_files(os.path.join(data_dir,str(year)),[".csv"])
        flist = filter_flist(flist)
        if len(flist) > 0:
            df_list = get_df_list(flist,data_col,rename_col)
            end_dfs.append(pd.concat(df_list,sort = True).sort_index())
    end_dfs = [df.to_frame() for df in end_dfs]
    end_dfs = [df.loc[~df.index.duplicated(keep='first')] for df in end_dfs]
    for ender in end_dfs:
        print(ender.columns.values)
        print(len(ender))
        print(len(pd.unique(ender.index.values)))
    
    df = pd.concat(end_dfs,axis = 1).resample("1H").mean()
##    df = end_dfs[0]
##    df = df.join(end_dfs[1],how = "outer")
##    df = df.join(end_dfs[2],how = "outer")
##    df = df.resample("1H").mean()
    output_df(df,output_dir,year)


if __name__ == "__main__":
    df = run_me()
    
