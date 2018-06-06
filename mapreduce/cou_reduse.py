import sys





def read_map_output():
    """This function use the read map output and spilt the data to two list base on the countrycode
        and  delete the redundancies
        then invoke the calculatelist to give the final result
        Input format:category \t (videoid;country;countrycode)
        Output format: calculatelist(list1,list2) 
                       list1 and list2 in format (category,videoid,country,countrycode)
    """

    data=[]
    for line in sys.stdin:
        templist=line.strip().split("\t")
        detailslist=templist[1].strip().split(";")
        newline=[templist[0],detailslist[0],detailslist[1],detailslist[2]]
        data.append(newline)

    list1=[]
    list2=[]
   
    # spilt the data to two list base on the countrycode delete the redundancies 
    for row in data:
            if row[3]=="1" and row not in list1:
                list1.append(row)
            elif row[3]=="2" and row not in list2:
                list2.append(row)
    calculatelist(list1,list2)



def calculatelist(list1,list2):
    """This function read the lists from read_map_output and caculte the result
        Input format:list1,list2
        Output format: catergory;total: numbers; persent in country2. 
                   
    """
    categorys=[]
    categotylist=[]
    final_list=[]
    new_list2=[]
    second_categorys=[]

    # get the country name of country1 and country2
    country1=list1[0][2]
    country2=list2[0][2]

    # find which row of (videoed, category) of list2 is also in list1
    for row in list2:
        row[2]=country1
        row[3]="1"
        if row in list1:
            new_list2.append(row)
            second_categorys.append(row[0])

    # put all the categories in list1 together as an array    
    for line in list1:
        categorys.append(line[0])

    # find the unique categories
    categotylist=set(categorys)

    # for every category find the count number in both categorys and second_categorys
    # so we have the times of video trending in the both two country based on the category
    # calculate the final result and store the output as required format in final_list
    for cate in categotylist:
        countnumber=categorys.count(cate)
        secondnumber=second_categorys.count(cate)
        percent=secondnumber/countnumber*100
        newline=cate+";"+str(countnumber)+";"+str(round(percent,1))+"%"
        final_list.append(newline)

    # print the result to the output file
    for items in final_list:
        details=items.split(";")
        print(details[0]+"; total: "+details[1]+"; "+details[2]+" in "+country2+"\n")


if __name__ == "__main__":
    read_map_output()
