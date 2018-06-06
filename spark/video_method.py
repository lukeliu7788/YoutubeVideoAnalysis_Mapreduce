#!/usr/bin/env python2
# -*- coding:utf-8 -*-
import csv

def mapperoriganialdata(record):
    """ This map function use to read the input 
    Input format: videoid,...,category,.....,country
    Output format: ((country,video_id), trending_date, views)
    """
    cvslist=list(csv.reader([record]))
    try:
        for item in cvslist:
            video_id=item[0]
            ortrending_date=item[1]
            views=int(item[8])
            country=item[-1]
            # put country and video_id together as the key
            key=country+";"+video_id
            # change the date colume into integer format： year+month+day
            trending_datelist=ortrending_date.split(".")
            trending_date=int(trending_datelist[0])*10000+int(trending_datelist[1])+int(trending_datelist[2])*100
            return (key,trending_date,views)
    except:
            return None


def mapfirst(line):
    """ This map function use to find the first appearance of a video 
    Input format: ((country,video_id), trending_date_list)
    Output format: ((country,video_id, first_appearance),0)
    """
    try:
        key,time = line
        time.sort()
        finalkey=key+";"+str(time[0])
        return (finalkey,0)
    except:
        return None


def mapsecond(line):
    """ This map function use to find the second appearance of a video 
    Input format: ((country,video_id), trending_date_list)
    Output format: ((country,video_id, second_appearance),0)
    """
    try:
        key,time = line
        time.sort()
        finalkey=key+";"+str(time[1])
        return (finalkey,0)
    except:
        return None

def mapchangekey(line):
    """ This map function use to change the  key of the element 
    Input format: ((country,video_id), trending_date, views)
    Output format: ((country,video_id, trending_date), views)
    """
    try:
        key,time,views = line
        finalkey=key+";"+str(time)
        return (finalkey,views)
    except:
        return None

def changekeyback(line):
    """ This map function use to change the key of the element 
    Input format: ((country,video_id, trending_date), [0,views])
    Output format: ((country,video_id), views)
    """
    try:
        key,values= line
        keylist=key.split(";")
        finalkey=keylist[0]+"; "+keylist[1]
        finalvalue=values[0]+values[1]
        return (finalkey,finalvalue)
    except:
        return None

def finalmap(line):
    """ This map function use to change the key of the element 
    Input format: ((country, video_id), [firstviews, secondviews])
    Output format: ((country,video_id), persentage)
    """
    try:
        key,views = line
        number1=float(views[0])
        number2=float(views[1])
        comparerate=(number2-number1)/number1*100
        # filter the videos which increase more than or equal to 1000%
        if comparerate>=1000:
            return (key,round(comparerate,1))
        else:
            return None
    except:
        return None

def nonefilter(line):
    """ This filter function use to read the input delect the none element in the RDD
    """
    if line!=None:
        return True
    else:
        return False
    
    
    
    
