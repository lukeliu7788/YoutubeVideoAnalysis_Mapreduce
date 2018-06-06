#!/usr/bin/env python2
# -*- coding:utf-8 -*-

from pyspark import SparkContext
from video_method import *
import argparse


if __name__ == "__main__":
    sc = SparkContext(appName="Video Trend Analysis")
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="the input path",
                        default='input')
    parser.add_argument("--output", help="the output path", 
                        default='Result') 
    args = parser.parse_args()
    input_path = args.input
    output_path = args.output
    videos1 = sc.textFile(input_path + "/ALLvideos.csv",use_unicode=False)

    # read the input file and map it using mapperoriganialdata function then delete the none elemet
    video_selected=videos1.map(mapperoriganialdata).filter(nonefilter)

    # change the video_selected into ((country,video_id), trending_date) and group it by key
    # videslist data format ((country,video_id), trending_date_list))
    videslist=video_selected.map(lambda x:x[:-1]).groupByKey().mapValues(list)

    # find all the (country,video_id) and its first first appearance and change the key
    # fisttrending  data format ((country,video_id,first_tending_data),0)
    fisttrending=videslist.map(mapfirst)

    # find all the (country,video_id) and its second appearance 
    # use filter to delete the videos dose not have the second appearance
    # secondtrending data format ((country,video_id,second_tending_data),0)
    secondtrending=videslist.map(mapsecond).filter(nonefilter) 

    # change the key video_doublekeylist  data format ((country,video_id,second_tending_data),views)
    video_doublekeylist=video_selected.map(mapchangekey)

    # join video_doublekeylist with fisttrending ,secondtrending and change the key back
    firstviewslist=video_doublekeylist.join(fisttrending).map(changekeyback)
    secondviewslist=video_doublekeylist.join(secondtrending).map(changekeyback)

    # join firstviewslist with secondviewslist
    # finalcomparelist data format ((country, video_id), [firstviews, secondviews])
    finalcomparelist=firstviewslist.join(secondviewslist)

    #using finalcomparelist to caculate the result
    finalprintlist=finalcomparelist.map(finalmap).filter(nonefilter)

    #sort the order by percent increase and country
    finalprintlist=finalprintlist.coalesce(1)
    finalprintlist=finalprintlist.sortBy(lambda x:x[1],ascending=False).sortBy(lambda x:x[0][0:1])
    
    #save the result
    finalprintlist.map(lambda x: x[0]+", "+str(x[1])+"%").coalesce(1).saveAsTextFile(output_path)