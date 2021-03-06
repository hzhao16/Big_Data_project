from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from pyspark.sql import Row
from pyspark.sql import SQLContext
from datetime import datetime
import re

def column_type(column, basic_type, semantic_type, column_name):
    output = column.map(lambda x: basic_type + '\t' + semantic_type + '\t' +  check_valid(x, column_name))
    output.saveAsTextFile('Type_{0}.out'.format(column_name))	
   
def check_valid(x, semantic_type):
    if semantic_type == 'Unique_Key':
    	if x == '' or x == 'N/A': 
            return 'NULL'
        elif x == 'N/A':
            return 'Non-empty missing data'
    	else:
    	    try:
    	        x = int(x)
    	        return 'Valid'
    	    except:
     	        return 'Invalid'

    elif semantic_type == 'Created_Date':
        if x == '':
            return 'NULL'
        if  x == 'N/A' or x == 'Unspecified':
            return 'Non-empty missing data'
    	try:
    	    date = datetime.strptime(x, '%m/%d/%Y %I:%M:%S %p')
    	    if date.year <= 2017 and date.year >= 2010:
                return 'Valid'
    	    else:
                return 'Invalid'
    	except:
    	    return 'Invalid'
    
    elif semantic_type == 'Closed_Date':
        if x == '':
            return 'NULL'
        if x == 'N/A' or x == 'Unspecified':
            return 'Non-empty missing data'    
        try:
            date = datetime.strptime(x, '%m/%d/%Y %I:%M:%S %p')
            if date.year <= 2017 and date.year >= 2010:
                return 'Valid'
            else:
                return 'Invalid'
        except:
            return 'Invalid' 
 
    elif semantic_type == 'Agency':
        if x == '':
            return 'NULL'
        elif x == 'N/A' or x == 'Unspecified':
            return 'Non-empty missing data'
        else:
            return 'Valid'

    elif semantic_type == 'Agency_Name':
        if x == '':
            return 'NULL'
        elif x == 'N/A' or x == 'Unspecified':
            return 'Non-empty missing data'    
        else:
            return 'Valid'

    elif semantic_type == 'Complaint_Type':
        if x == '':
            return 'NULL'
        elif x == 'Unspecified':
            return 'Non-empty missing data'    
        else:
            return 'Valid'
	
    elif semantic_type == 'Descriptor':
        if x == '':
            return 'NULL'
        elif  x == 'N/A' or x == 'Unspecified':
            return 'Non-empty missing data'
        else:
            return 'Valid'

    elif semantic_type == 'Location_Type':
        if x == '':
            return 'NULL'
        elif x == 'N/A' or x == 'Unspecified':
            return 'Non-empty missing data'
        else:
            return 'Valid'

    elif semantic_type == 'Incident_Zip':
        if x == '':
            return 'NULL'
        elif x == 'N/A' or x == 'NA' or x == 'Unknown' or x == 'N.A' or x == 'unsure':
            return 'Non-empty missing data'
        else:
    	    x = x.strip()
    	    if re.match('^\d{5}(?:[-\s]\d{4})?$', x) is None:
    		return 'Invalid'
    	    else:
    		if int(x[:5]) < 501 or int(x[:5]) > 99950:
    		    return 'Invalid'
    		else:
    		    return 'Valid'

    elif semantic_type == 'Incident_Address':
        if x == '':
            return 'NULL'
        elif x.upper().startswith('N/A') or x.upper().endswith('N/A') or x == 'Unspecified':
            return 'Non-empty missing data'
        else:
            x = x.replace(' ', '')
            try:
                x = int(x)
                return 'Invalid'
            except:
                if len(x) <= 5:
                    return 'Invalid'
                else:
                    if re.search('[!@#\$%^&*]+', x) is None:
                        return 'Valid'
                    else:
                        return 'Invalid'

    elif semantic_type == 'Street_Name':
        if x == '':
            return 'NULL'
        elif x == 'N/A' or x == 'N/A/' or x == 'Unspecified' or x == '9999':
            return 'Non-empty missing data'
        else:
            x = x.replace(' ', '')
            try:
                x = int(x)
                return 'Invalid'
            except:
                if len(x) <= 5:
                    return 'Invalid'
                else:
                    if re.search('[!@#\$%^&*]+', x) is None:
                        return 'Valid'
                    else:
                        return 'Invalid'
	
    elif semantic_type == 'Cross_Street_1':
        if x == '':
            return 'NULL'
        elif x == 'N/A'or x == 'UNNAMED ST' or x == 'UNNAMED STREET':
            return 'Non-empty missing data'
    	else:
    		x = x.replace(' ', '')
    		try:
    		    x = int(x)
    		    return 'Invalid'
    		except:
    		    if len(x) <= 5:
                    return 'Invalid'
    		    else:
                    if re.search('[!@#\$%^&*]+', x) is None:
                        return 'Valid'
                    else:
                        return 'Invalid'  

    elif semantic_type == 'Cross_Street_2':
        if x == '':
            return 'NULL'
        elif x == 'N/A' or x == 'UNNAMED STREET' or x == 'UNNAMED ST' or x == 'UNNAMED STREET/UNNAMED STREET': 
            return 'Non-empty missing data' 
        else: 
            x = x.replace(' ', '') 
            try: 
                x = int(x) 
                return 'Invalid' 
            except: 
                if len(x) <= 5: 
                    return 'Invalid' 
                else: 
                    if re.search('[!@#\$%^&*]+', x) is None: 
                        return 'Valid'  
                    else:  
                        return 'Invalid'   

    elif semantic_type == 'Intersection_Street_1':
        if x == '' :
            return 'NULL'
        elif x == 'UNNAMED STREET' or x == 'N/A': 
            return 'Non-empty missing data' 
        else: 
            x = x.replace(' ', '') 
            try: 
                x = int(x) 
                return 'Invalid' 
            except: 
                if len(x) <= 5: 
                    return 'Invalid' 
                else: 
                    if re.search('[!@#\$%^&*]+', x) is None: 
                        return 'Valid'  
                    else:  
                        return 'Invalid'   

    elif semantic_type == 'Intersection_Street_2':
        if x == '':
            return 'NULL'
        elif x == 'UNNAMED STREET' or 'UNNAMED ST' or x == 'N/A': 
            return 'Non-empty missing data' 
        else: 
            x = x.replace(' ', '') 
            try: 
                x = int(x) 
                return 'Invalid' 
            except: 
                if len(x) <= 5: 
                    return 'Invalid' 
                else: 
                    if re.search('[!@#\$%^&*]+', x) is None: 
                        return 'Valid'  
                    else:  
                        return 'Invalid'   

    elif semantic_type == 'Address_Type':
        if x == '':
            return 'NULL'
        else:
	       return 'Valid'

    elif semantic_type == 'City':
        if x == '':
            return 'NULL'
        elif x == 'N/A' or x == 'NA' or x == 'Unknown':
            return 'Non-empty missing data'
        else:
            x = x.strip().replace(' ', '')
            if len(x) <= 2:
                return 'Invalid'
    	    try:
                x = int(x)
                return 'Invalid'
    	    except:
                if re.search('[!@#\$%^&*-]+', x) is None:
                    return 'Valid'
                else:
                    return 'Invalid'
 
    elif semantic_type == 'Landmark':
        if x == '':
            return 'NULL'
        elif x == 'N/A':
            return 'Non-empty missing data'
        else:
            return 'Valid'

    elif semantic_type == 'Facility_Type':
        if x == '':
            return 'NULL'
        elif x == 'N/A':
            return 'Non-empty missing data'
        else:
            return 'Valid'

    elif semantic_type == 'Status':
        if x == '':
            return 'NULL'
        elif x.upper() == 'UNSPECIFIED':
            return 'Non-empty missing data'
        else:
            return 'Valid'

    elif semantic_type == 'Due_Date':
        if x == '':
            return 'NULL'
        try:
            date = datetime.strptime(x, '%m/%d/%Y %I:%M:%S %p')
            if date.year <= 2017 and date.year >= 2010:
                return 'Valid'
            else:
                return 'Invalid'
        except:
            return 'Invalid'
        
    elif semantic_type == 'Resolution_Action_Updated_Date':
        if x == '':
            return 'NULL'
        try:
            date = datetime.strptime(x, '%m/%d/%Y %I:%M:%S %p')
            if date.year <= 2017 and date.year >= 2010:
                return 'Valid'
            else:
                return 'Invalid'
        except:
            return 'Invalid'
        
    elif semantic_type == 'Community_Board':
        if x.upper() == '0 UNSPECIFIED':
            return 'Non-empty missing data'
        else:
            return 'Valid'

    elif semantic_type == 'Borough':
        if x == 'Unspecified':
            return 'Non-empty missing data'
        else:
            return 'Valid'

    elif semantic_type == 'X_Coordinate_State_Plane':
        if x == '':
            return 'NULL'
        else:
            return 'Valid'

    elif semantic_type == 'Y_Coordinate_State_Plane':
        if x == '':
            return 'NULL'
        else:
            return 'Valid'

    elif semantic_type == 'Park_Facility_Name':
        if x == '':
            return 'NULL'
        elif x == 'Unspecified':
            return 'Non-empty missing data'
        else:
            return 'Valid'

    elif semantic_type == 'Park_Borough':
        if x == '':
            return 'NULL'
        elif x == 'Unspecified':
            return 'Non-empty missing data'
        else:
            return 'Valid'

    elif semantic_type == 'School_Name':
        if x == '':
            return 'NULL'
        elif x == 'Unspecified':
            return 'Non-empty missing data'
        else:
            return 'Valid'

    elif semantic_type == 'School_Number':
        if x == '':
            return 'NULL'
        elif x == 'N/A' or x == 'Unspecified':
            return 'Non-empty missing data' 
        else:
            return 'Valid'

    elif semantic_type == 'School_Region':
        if x == '':
            return 'NULL'
        elif x == 'Unspecified':
            return 'Non-empty missing data'
        else:
            return 'Valid'

    elif semantic_type == 'School_Code':
        if x == '':
            return 'NULL'
        elif x == 'Unspecified':
            return 'Non-empty missing data'
        else:
            return 'Valid'

    elif semantic_type == 'School_Phone_Number':
        if x == 'NA 0/0' or x == 'Unspecified':
            return 'Non-empty missing data'
        else:
            return 'Valid'	
	
    elif semantic_type == 'School_Address':
        if x == 'Unspecified':
            return 'Non-empty missing data'
        else:
            return 'Valid'

    elif semantic_type == 'School_City':
        if x == 'Unspecified':
            return 'Non-empty missing data'
        else:
            return 'Valid'

    elif semantic_type == 'School_State':
        if x == 'Unspecified':
            return 'Non-empty missing data'
        else:
            return 'Valid'

    elif semantic_type == 'School_Zip':
        if x == 'N/A' or x == 'Unspecified':
            return 'Non-empty missing data'
        else:
            return 'valid'

    elif semantic_type == 'School_Not_Found':
        if x == '':
            return 'NULL'
        else:
            return 'Valid'

    elif semantic_type == 'School_or_Citywide_Complaint':
        if x == '':
            return 'NULL'
        else:
            return 'Valid'

    elif semantic_type == 'Vehicle_Type':   
        if x == '':
            return 'NULL'
        else:
            return 'Valid'

    elif semantic_type == 'Taxi_Company_Borough':
        if x == '':
            return 'NULL'
        else:
            return 'Valid'

    elif semantic_type == 'Taxi_Pick_Up_Location':
        if x == '':
            return 'NULL'
        else:
            return 'Valid' 

    elif semantic_type == 'Bridge_Highway_Name':
        if x == '':
            return 'NULL'
        else:
            return 'Valid' 

    elif semantic_type == 'Bridge_Highway_Direction':
        if x == '':
            return 'NULL'
        elif x == 'N/A':
            return 'Non-empty missing data'    
        else:
            return 'Valid'

    elif semantic_type == 'Road_Ramp':
        if x == '':
            return 'NULL'
        elif x == 'N/A':
            return 'Non-empty missing data'    
        else:
            return 'Valid'

    elif semantic_type == 'Bridge_Highway_Segment':
        if x == '':
            return 'NULL'
        elif x == 'N/A':
            return 'Non-empty missing data' 
        else:
            x = x.replace('-', '')
            try:
                x = int(x)
                return 'Invalid'
            except:
                return 'Valid'

    elif semantic_type == 'Garage_Lot_Name':
        if x == '':
            return 'NULL'
        else:
            return 'Valid'

    elif semantic_type == 'Ferry_Direction':
        if x == '':
            return 'NULL'
        elif x == 'N/A':
            return 'Non-empty missing data'    
        else:
            return 'Valid'

    elif semantic_type == 'Ferry_Terminal_Name':
        if x == '':
            return 'NULL'
        elif x == 'N/A' or x == 'NA' or x == 'Unknown':
            return 'Non-empty missing data' 
        else:
            return 'Valid'

    elif semantic_type == 'Latitude':
        if x == '':
            return 'NULL'
    	else:
            try:
                x = float(x)
                if x < 40.487470 or x > 40.920000:
                    return 'Invalid'
                else:
                    return 'Valid'
            except: 
                return 'Invalid'

    elif semantic_type == 'Longitude':
        if x == '':
            return 'NULL'
        else:
            try:
                x = float(x)
                if x < -74.257301 or x > -73.695160:
                    return 'Invalid'
                else:
                    return 'Valid'
            except:
                return 'Invalid'

    elif semantic_type == 'Location':
        if x == '':
            return 'NULL'
    	else:	
    	    try:
    	    	x = x.strip().replace('(', '').replace(')','').split(',')
    	    	lat = float(x[0].strip())
    	    	lng = float(x[1].strip())
                if lat < 40.487470 or lat > 40.920000:
                    return 'Invalid'
                elif lng < -74.257301 or lng > -73.695160:
                    return 'Invalid'
                else:
                    return 'Valid'
            except:
                return 'Invalid'		

def main():
    sc = SparkContext()
   
    #csvfile = sc.textFile('/user/hw1567/big_data_project_dataset/311.csv',1)
    csvfile = sc.textFile('/user/jub205/311all.csv',1)

    header = csvfile.first()

    data = csvfile.filter(lambda x: x != header).map(lambda x:x.encode('utf-8','ignore')).mapPartitions(lambda line: reader(line))

    column_txt = sc.textFile('/user/jub205/columns.txt').map(lambda line: line.split('|'))
    #column_txt = sc.textFile('/user/hw1567/big_data_project_dataset/columns.txt').map(lambda line: line.split('|'))
    column_name = column_txt.map(lambda line: line[0]).collect()
    column_basic_type = column_txt.map(lambda line: line[2]).collect()
    column_semantic_type = column_txt.map(lambda line: line[3]).collect()
    
    for i in range(52):
		column_data = data.map(lambda x: x[i])
		column_type(column_data, column_basic_type[i], column_semantic_type[i], column_name[i])

    sc.stop()


if __name__ == "__main__":
	main()
