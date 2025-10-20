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

def filter_flist(flist,year,bad_files):
    flist = list(set(flist) - set(bad_files))
    start_date = int(str(year-1)[2:] + "1229")
    end_date = int(str(year+1)[2:] + "0102")
    flist = [i for i in flist if ((int(i[-13:-7]) >= start_date) and (int(i[-13:-7]) <= end_date))]
    return flist

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
        df = pd.read_csv(fname,index_col = 0,parse_dates = True,date_parser = get_date)
    except Exception as e:
        df = str(fname) + "\t" + str(e)
    return df

def get_date(val):
    return xlrd.xldate_as_datetime(float(val),0).replace(microsecond = 0)

def split_df_list(df_list):
    successes = [i for i in df_list if isinstance(i,pd.DataFrame)]
    fails = [i for i in df_list if not isinstance(i,pd.DataFrame)]
    return (fails,successes)

def drop_dates(df,date_list):
    for i in date_list.to_dict("records"):
        try:
            df = df.drop(df[i["Start Date"]:i["End Date"]].index.values)
        except:
            pass
    return df

def correct_col_names(df,name_list):
    name_list["End Date"] = name_list["Date"].shift(-1)
    name_list = name_list.to_dict("records")
    for i in name_list:
        if pd.isnull(i["End Date"]):
            ender = datetime.datetime.now()
        else:
            ender = i["End Date"]
        try:
            df.loc[i["Date"]:ender,"Primary Instrument"] = df.loc[i["Date"]:ender,i["Primary Instrument"]]
            df.loc[i["Date"]:ender,"Secondary Instrument"] = df.loc[i["Date"]:ender,i["Secondary Instrument"]]
        except Exception as e:
            pass
    df = df.drop([i for i in df.columns.values if i not in ["Primary Instrument","Secondary Instrument"]],axis = 1)
    return df

def flag_dataframe(df):
    df["Primary Instrument Flags"] = 1
    df["Secondary Instrument Flags"] = 1
    df.loc[df["Secondary Instrument"]>80,"Secondary Instrument Flags"] = 3
    df.loc[df["Secondary Instrument"]<10,"Secondary Instrument Flags"] = 2
    df.loc[df["Secondary Instrument"]<1,"Secondary Instrument Flags"] = 5
    df.loc[(abs(((df["Primary Instrument"]-df["Secondary Instrument"])/df["Primary Instrument"])) > 0.15) & (df["Secondary Instrument Flags"] ==0),"Primary Instrument Flags"] = 2
    df.loc[df["Primary Instrument"]>80,"Primary Instrument Flags"] = 3
    df.loc[df["Primary Instrument"]<10,"Primary Instrument Flags"] = 2
    df.loc[df["Primary Instrument"]<1,"Primary Instrument Flags"] = 5
    #only one instrument not working use flag 4
    return df

def output_df(df,output_dir,year):
    create_folder([output_dir,year])
    for i in pd.unique(df[df.index.year == year].index.month):
        create_folder([output_dir,year,calendar.month_name[i]])
        output_month(df,output_dir,year,i)

def create_folder(path_parts):
    os.makedirs(os.path.join(*list(map(str,path_parts))),exist_ok = True)

def output_month(df,output_dir,year,month):
    df[((df.index.year == year) & (df.index.month == month))].to_csv(os.path.join(*list(map(str,[output_dir,year,calendar.month_name[month],calendar.month_name[month]]))) + "_" + str(year) + "_Ozone.csv")

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
 
def run_me(year = None):
    if year == None:
        year = int(datetime.datetime.now().year)
    curr_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    conf_dir = os.path.join(curr_dir,"config")
    #parent_dir = str(pathlib.Path(curr_dir).parent)
    data_dir = r"/gws/nopw/j04/ncas_obs/amf/raw_data/ncas-49i-o3-1/data"#os.path.join(parent_dir,"Input_data")
    output_dir = r"/gws/nopw/j04/ncas_obs/amf/processing/ncas-49i-o3-1/output"#os.path.join(parent_dir,"Output")
    
    excluded_dates = read_conf_file(os.path.join(conf_dir,"remove_date_times.mww"))
    instrument_dates = read_conf_file(os.path.join(conf_dir,"instrument_names.mww"))
    bad_files = read_conf_file_list(os.path.join(conf_dir,"bad_files.mww"),"\t",0)
    
    flist = glob.glob(data_dir + os.path.sep + "*daily_minute*")
    flist = filter_flist(flist,year,bad_files)
    df_list = get_df_list(flist)
    error_list,df_list = split_df_list(df_list)
    giant_df = pd.concat(df_list,sort = True).sort_index()
    giant_df = drop_dates(giant_df,excluded_dates)
    giant_df = correct_col_names(giant_df,instrument_dates)
    giant_df = flag_dataframe(giant_df)
    output_df(giant_df,output_dir,year)
    if len(error_list) > 0:
        output_errors(os.path.join(output_dir,str(year),"file_errors.txt"),error_list)

if __name__ == "__main__":
    df = run_me()
    
