#! /usr/bin/env python
#  -*- coding: utf-8 -*-
#
# Support module generated by PAGE version 4.19
#  in conjunction with Tcl version 8.6
#    Mar 11, 2019 10:32:54 AM GMT  platform: Windows NT

import sys
import os
import pathlib
import inspect
import pandas as pd

try:
    import Tkinter as tk
except ImportError:
    import tkinter as tk

try:
    import ttk
    py3 = False
except ImportError:
    import tkinter.ttk as ttk
    py3 = True

def set_Tk_var():
    global date_add_instr
    date_add_instr = tk.StringVar()
    global primary_add_instr
    primary_add_instr = tk.StringVar()
    global secondary_add_instr
    secondary_add_instr = tk.StringVar()
    global start_remove_dates
    start_remove_dates = tk.StringVar()
    global end_remove_dates
    end_remove_dates = tk.StringVar()
    global reason_remove_dates
    reason_remove_dates = tk.StringVar()
    
def add_instr_name():
    mylist = [date_add_instr.get(),primary_add_instr.get(),secondary_add_instr.get()]
    add_to_treeview(mylist,w.treeview_instr_names,bool_sort = True)
    

def add_remove_dates():
    mylist = [start_remove_dates.get(),end_remove_dates.get(),reason_remove_dates.get()]
    add_to_treeview(mylist,w.treeview_remove_dates,bool_sort = True)

def finish():
    print('ozone_gui_support.finish')
    sys.stdout.flush()

def remove_instr_name():
    remove_from_treeview(w.treeview_instr_names)

def remove_remove_dates():
    remove_from_treeview(w.treeview_remove_dates)

def save_instr_name():
    mydf = get_df_tv(w.treeview_instr_names)
    write_out(os.path.join(conf_dir,"instrument_names.mww"),mydf)

def save_remove_dates():
    mydf = get_df_tv(w.treeview_remove_dates)
    write_out(os.path.join(conf_dir,"remove_date_times.mww"),mydf)

def init(top, gui, *args, **kwargs):
    global w, top_level, root, conf_dir
    w = gui
    top_level = top
    root = top
    curr_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    conf_dir = os.path.join(curr_dir,"config")
    populate_form()

def destroy_window():
    # Function which closes the window.
    global top_level
    top_level.destroy()
    top_level = None
    
def add_to_treeview(to_add,to_add_to,bool_sort = False,sort_col = 1):
    to_add_to.insert("","end",values = to_add)
    if bool_sort:
        sort_tv(to_add_to,sort_col)
        
def remove_from_treeview(tv):
    if tv.selection():
        tv.delete(tv.selection())
        
def sort_tv(child,sort_col):
    l =[]
    for item in child.get_children():
        l.append((child.item(item,option= "text"),child.set(item)))
    sorted_list = sorted(l, key=lambda item: item[1]["Col" + str(sort_col)])
    for myitem in child.get_children():
        child.delete(myitem)
    for item in sorted_list:
        child.insert("","end")
        for k,v in item[1].items():
            child.set(child.get_children()[-1],k,v)
            
def get_df_tv(tv,set_names_and_vals = None):
    col_names = get_tv_col_names(tv)
    if tv.get_children():
        l = []
        for item in tv.get_children():
            l.append(tv.set(item))
        if l:
            mini_df = pd.DataFrame(data = l)#,columns = col_names)
            mini_df.columns = col_names
            if set_names_and_vals:
                for i in set_names_and_vals:
                    mini_df[i[0]] = i[1]
    else:
        mini_df = pd.DataFrame(columns = (col_names + [i[0] for i in set_names_and_vals]))
    return mini_df

def reset_tv(tv,filename,filter_column = None,filter_value = None):
    tv.delete(*tv.get_children())
    mydf = get_file_df(filename)
    if mydf.empty == False:
        fill_tv_from_df(tv,mydf,filter_column,filter_value)
        
def populate_form():
    reset_tv(w.treeview_instr_names,os.path.join(conf_dir,"instrument_names.mww"))
    reset_tv(w.treeview_remove_dates,os.path.join(conf_dir,"remove_date_times.mww"))
               
def get_file_df(filename):
    if os.path.isfile(filename):
        try:
            mydf = pd.read_csv(filename,index_col = False,sep = "\t")
        except:
            mydf = pd.DataFrame()
    else:
        if not os.path.isdir(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))
        open(filename, 'a').close()
        mydf = pd.DataFrame()
    return mydf

def fill_tv_from_df(tv,mydf,filter_column,filter_value):
    if filter_column:
        mydf = mydf[mydf[filter_column] == filter_value]
    
    if mydf.empty == False:
        col_names = get_tv_col_names(tv)
        for index,row in mydf.iterrows():
            to_add = get_row_vals(col_names,row)
            add_to_treeview(to_add,tv)
    
def get_row_vals(col_names,row):
    vals = []
    for i in col_names:
        try:
            vals.append(row[i])
        except:
            pass
    return vals
    
def get_tv_col_names(tv):
    col_names = []
    i = 0
    while True:
        try:
            col_names.append(tv.heading(i,"text"))
        except:
            break
        i += 1
    return col_names

def write_out(filename,df,add_to = False,filter_column = None,filter_value = None):
    if add_to == True:
        mydf = get_file_df(filename)
        if mydf.empty == False:
            if filter_column:
                mydf = mydf[mydf[filter_column]!=filter_value]
            df = pd.concat([df,mydf])
    df.to_csv(filename,index = False,sep = "\t")

if __name__ == '__main__':
    import ozone_gui
    ozone_gui.vp_start_gui()




