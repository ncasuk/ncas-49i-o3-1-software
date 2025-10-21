# args exected
#1. full path to file to be converted
#2. output diretory 
#3. path to meta file

#import sys
#import os
#import numpy as np
from collections import namedtuple
from cvo_o3_parser_v4 import cvo_o3_get_file_v1, cvo_o3_parse_data_v1
from cvo_o3_NC_v4 import cvo_o3_get_meta_v1, NC_cvo_o3_v1
import argparse

parser = argparse.ArgumentParser(description="CVAO Ozone data parser")
parser.add_argument('-o', '--outdir', default='./processed', help='Directory for output NetCDF files', dest='dout')
parser.add_argument('datafiles', nargs='+')

args = parser.parse_args()

#path to file out
dout = args.dout
data = namedtuple("data", "DT DoY ET O3 flag")
#path to file in
for fin in args.datafiles:

    #"/AMF_Netcdf/"
    #path to file metadata
    #fn_meta = str(sys.argv[3])
    fn_meta = fin[0:-12]+'meta_'+fin[-12:]

    #get the data
    data.DT, data.DoY, data.ET, data.O3, data.flag = cvo_o3_get_file_v1(fin)

    #parse data - make sure just one month of data
    data.DT, data.DoY, data.ET, data.O3, data.flag = cvo_o3_parse_data_v1(data)
 
    #save nc file
    meta = cvo_o3_get_meta_v1(fn_meta)
    NC_cvo_o3_v1(meta, dout, data)

#NB To run this use terminal window.  Go to drive where file and metadata are. Then run.
#/Google Drive/katie_computer/Katie_documents/Dan
# python ozone/cvo_o3_v2.py cv-tei-o3_capeverde_2018??01.txt