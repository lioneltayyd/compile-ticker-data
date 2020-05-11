

# Python modules.
import regex as re
import pandas as pd


# ----------------------------------------------------------------------
# Write into Excel file.  
# ----------------------------------------------------------------------

def singleSheetMultiWrite(excelWriter, pivot_dict, pivot_stats, sheetName, keys, 
                          startcol, startrow, distance):
    
    startrow1st = startrow
    
    for i, key in enumerate(keys):
        pivot_dict[key].to_excel(excelWriter, sheet_name=sheetName, startcol=startcol, startrow=startrow1st)
        startcol2nd = startcol + len(pivot_dict[key].columns) + distance
        
        re_compile = re.compile(f'(?:{key})(?:_R.*Yr)?(?!_)')
        stats_keys = list(filter(re_compile.match, pivot_stats.keys()))
        
        for stats_key in stats_keys:
            pivot_stats[stats_key].to_excel(excelWriter, sheet_name=sheetName, 
                                            startcol=startcol2nd, startrow=startrow1st)
            startcol2nd = startcol2nd + len(pivot_stats[stats_key].columns) + distance
        
        startrow1st = startrow1st + len(pivot_dict[key].index) + distance 

        
def multiSheetWrite(excelWriter, pivot_dict, pivot_stats, sheetName, key, 
                    startcol, startrow, distance):    

    pivot_dict[key].to_excel(excelWriter, sheet_name=sheetName, startcol=startcol, startrow=startrow)
    startcol2nd = startcol + len(pivot_dict[key].columns) + distance

    re_compile = re.compile(f'(?:{key})(?:_R.*Yr)?(?!_)')
    stats_keys = list(filter(re_compile.match, pivot_stats.keys()))
    
    for stats_key in stats_keys:
        pivot_stats[stats_key].to_excel(excelWriter, sheet_name=sheetName, 
                                        startcol=startcol2nd, startrow=startrow) 
        startcol2nd = startcol2nd + len(pivot_stats[stats_key].columns) + distance