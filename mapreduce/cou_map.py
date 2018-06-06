import sys
import csv
import os

def cou_mapper():  
    """This mapper resive the input from the csv in line and return the category,videoid,country,countrycode information
        Input format: videoid,...,category,.....,country
        Output format:  filter(country1,country2,result(a list which row format is (videoid;category;country)))
        Then invoke the filter function the give the output to reducer 
    """
    result=[]
    for line in sys.stdin:
        strline=str(line)
        csv_list= list(csv.reader([strline]))
        for row in csv_list:
                newline=[row[0]+";"+row[5]+";"+row[-1]]
                result.append(newline)
    country1=os.environ.get("country1")
    country2=os.environ.get("country2")
    result=result[1:]
    filter(country1,country2,result)


def filter(county1,county2,list):
    """This function get output from cou_mapper() and 
        filter the videos belongs to the both two country add another column called countrycode
        (1 means the first country,2 means the second country) 
        Input format:country1,country2,result(a list which row format is (videoid;category;country))
        Output format:category \t (videoid;country;countrycode)
    """

    for line in list:
        details=line[0].strip().split(";")
        if details[2]==county1:
            videoid=details[0]
            category=details[1]
            country=details[2]
            countrycode="1"
            value=videoid+";"+country+";"+countrycode
            print("{}\t{}".format(category,value))
        elif details[2]==county2:
            videoid=details[0]
            category=details[1]
            country=details[2]
            countrycode="2  "
            value=videoid+";"+country+";"+countrycode
            print("{}\t{}".format(category,value))
    
if __name__ == "__main__":
    cou_mapper()
    

