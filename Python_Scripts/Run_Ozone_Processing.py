import argparse
import Process_Ozone
#import Mirror_Data
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run Ozone processing for current year unless specified')
    parser.add_argument("-y","--year_to_do",default = "0",type=int,help='year to process')
    args = parser.parse_args()
    
    if int(args.year_to_do) > 0:
        year_to_process = int(args.year_to_do)
    else:
        year_to_process = None
    #Mirror_Data.run_me()
   
    Process_Ozone.run_me(year_to_process)
    
