#!/usr/bin/env python
import csv
import sys
import os

import pandas as pd
from datetime import datetime

from amfutils.instrument import AMFInstrument
from netCDF4 import Dataset

class ThermoO31(AMFInstrument):
    """
    Class to convert Thermo 49 Series Ozone Analyser data into netCDF format.
    Assumes Excel-style timestamp in data column `TheTime`
    """

    progname = __file__
    amf_variables_file = "o3-concentration.xlsx - Variables - Specific.csv"

    def get_data(self, infiles):
        """
            Example good dataline:

TheTime,ozone4_serial,ozone_4_ad,ozone2_serial,ozone_2_ad,temp,TempoC
43502.67864747,38.47000,122.3415,38.54000,15302.57,162.5043,0.4312147

            :param infiles: list(-like) of data filenames
            :return: a Pandas DataFrame of the noxy data
        """
        thermo = pd.DataFrame()
        for infile in infiles:
            with open(infile, 'rt') as f:
                thermo = pd.concat([thermo, pd.read_csv(f)])

        #Timeseries
        thermo['TheTime'] = pd.to_datetime('1899-12-30') + pd.to_timedelta(thermo.TheTime,'D') #Excel-ish time (decimal days since 1900-01-01, 1-indexed)
        #may not be correct for Jan-March01 1900 as Excel incorrectly thinks 1900
        #was a leap year
        thermo.set_index('TheTime', inplace=True)

        #set start and end times
        self.time_coverage_start = thermo.index[0].strftime(self.timeformat)
        self.time_coverage_end = thermo.index[-1].strftime(self.timeformat)

        self.rawdata = thermo
        return thermo

    def netcdf(self, output_dir):
        """
        Takes a dataframe (self.rawdata) with Thermo 49i O3 data and outputs a 
        well-formed NetCDF using appropriate conventions.

        :param output_dir: string contaiing path to output directory

        """

        self.setup_dataset('o3-concentration',1)

        #lat/long
        self.land_coordinates()
    
        #add all remaining attribs
        self.dataset.setncatts(self.raw_metadata)
    
        self.dataset.close()

if __name__ == "__main__":
    args = ThermoO31.arguments().parse_args()
    therm = ThermoO31(args.metadata) 

    try:
        os.makedirs(args.outdir,mode=0o755)
    except OSError:
         #Dir already exists, probably
         pass
    else:
        print ("Successfully create directory %s" % args.outdir)
    therm.get_data(args.infiles)
    therm.netcdf(output_dir=args.outdir)

