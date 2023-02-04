#!/usr/bin/env python
# coding: utf-8

# In[35]:


import pandas as pd
import xlrd
import os

#make a list of all HNRNP csv files
#First for loop cycles through files in HNRNP_gz folder *including those in subfolders*
#Second for loop grabs all SE files by getting all that start with 'SE.MATS'
#HNRNP.append adds file path to list HNRNP[]
HNRNP = []
for root, dirs, files in os.walk('/Users/meganholmes/Desktop/Documents/Hertel/ExonDB/tar.gz_files/HNRNP_gz'):
    for file in files:
        if file.startswith("SE.MATS"):
             HNRNP.append(os.path.join(root, file)) 
#make a list of all SR csv files
#First for loop cycles through files in SR_gz folder *including those in subfolders*
#Second for loop grabs all SE files by getting all that start with 'SE.MATS'
#b.append adds file path to list b[]
SR = []
for root, dirs, files in os.walk('/Users/meganholmes/Desktop/Documents/Hertel/ExonDB/tar.gz_files/SR_gz'):
    for file in files:
        if file.startswith("SE.MATS"):
             SR.append(os.path.join(root, file))

#combine list
paths = HNRNP + SR


# In[37]:


#Read paths as csv files and save to CSV_ouput folder
#pd.read_csv brings csv file into pandas data frame (SE)
#SE.to_csv creates csv file of SE in selected directory 
#paths[i] will iterate through each file path in list paths
#sep="\t" is for spacing and separation 
directory = '/Users/meganholmes/Desktop/Documents/Hertel/ExonDB/Intermediate_CSVs/SE.MATS_CSV/'
for i in range(len(paths)):
  SE = pd.read_csv(paths[0], sep="\t")
  SE.to_csv(directory + 'SE.MATS_' + str(i) + '.csv', index=None)


# In[34]:


import re

#Make list of KO names
#Find what is between _gz and -BG in file name and add it to list
#loops through list c
#re.search searches through a string
# .* will match any character
#? makes .* non-greedy (matching as little as it can to make a match) 
#the parenthesis makes it a capture group as well
#x.group(1) returns first subgroup of the match 
KO_names = []
for i in range(len(paths)):
    x = re.search('_gz/(.*?)-BG', c[i])
    KO = x.group(1) if x else None
    KO_names.append(KO)
print(KO_names)


# In[18]:


#Create csv files only containing rows for exonStart, exonEnd, pValue, FDR, Inclusion Level 1, Inclusion Level 2, and 
#Inclusion Level Difference
#exonEnd added to help clear out duplicates during join
#Directory2 is path for final files
#columns_wanted are in order above 
#Changes exonStart_base0 to exonStart to match big DB 
#Drop_duplicates is needed to get rid of duplicate exonStart and exonEnd matches so they don't compound during merge
#d is list of the paths to these edited files

directory2 = '/Users/meganholmes/Desktop/Documents/Hertel/ExonDB/Intermediate_CSVs/Selected/'
columns_wanted = [5, 6, 18, 19, 20, 21, 22]

#d is list of paths for later use
#Need sep="\t" or it doesn't fill properly 
d = []
for i in range(len(KO_names)):
    columns = ["exonStart", "exonEnd", KO_names[i]+' pValue',KO_names[i]+' FDR',
           KO_names[i]+' IncLev1',KO_names[i]+' IncLev2',KO_names[i]+' IncLevD']
    df = pd.read_csv(c[i], sep="\t", header = 0, usecols=columns_wanted, names=columns)
    df = df.drop_duplicates(subset = ['exonStart','exonEnd'], keep = 'first')
    df = df.drop_duplicates(subset = ['exonStart'], keep = 'first')
    df.to_csv(directory2 + "Selected_" + str(i) + '.csv', index=None)
    d.append(directory2 + "Selected_" + str(i) + '.csv')
    


# In[28]:


#Get exonStart and exonEnd columns of HEXevent 
#float_format='%.0f' gets rid of decimals in columns 
path = '/Users/meganholmes/Desktop/Documents/Hertel/ExonDB/File_link_tables/Starting/Exon_Data_Merged_with_Hexevent_v1.csv'
path_out = '/Users/meganholmes/Desktop/Documents/Hertel/ExonDB/File_link_tables/Intermediates/St_End_Exon_Data_Merged_with_Hexevent_v1.csv'
df = pd.read_csv(path,header = 0, usecols=[3,4], names=['exonStart', 'exonEnd'], float_precision=None)
df.to_csv(path_out,index = None, float_format='%.0f')


# In[31]:


#To start left join the first SE.MATS csv d[0] onto exonStart/exonEnd from HEXevent to remove same exonStart different
#exonEnd duplicates
#Then bring that result into loop and cycle through adding the rest of the SE.MATS files via left join
#m.extend fills list m with numerical values 1-30
#Used while loop here because for loop always starts on i = 1
#Left join because HEXevent db has a lot of exonStart/end values that SE.MATS files do not and not all SE.MATS files
#have the same exonStart/end values
i = 1
m = []
m.extend(range(1, 30))
directory3 ='/Users/meganholmes/Desktop/Documents/Hertel/ExonDB/Intermediate_CSVs/Individual_joins/'

data1 = pd.read_csv(path_out)
data2 = pd.read_csv(d[0])
m[i] = data1.merge(data2, 
                on=['exonStart','exonEnd'], 
                how='left')
m[i].to_csv(directory3 + "Build_Join_" + str(0) + '.csv', index=None)

j = 2
x = len(KO_names)
while i <= x:
#    data1 = pd.read_csv(d3, low_memory=False)
    data2 = pd.read_csv(d[i])
    m[j] = m[i].merge(data2, 
                on=['exonStart','exonEnd'], 
                how='left')
    m[j].to_csv(directory3 + "Build_Join_" + str(i) + '.csv', index=None)
    i = i + 1
    j = j + 1
  


# In[38]:


#Left join the full HEXevent db with the final db from above
directory4 ='/Users/meganholmes/Desktop/Documents/Hertel/ExonDB/Final_Join/Final_Join.csv'
data1 = pd.read_csv(path, low_memory=False)
data3 = pd.read_csv(directory3 + "Build_Join_" + str(19) + '.csv', low_memory=False)
    
output1 = data1.merge(data3, 
                on=['exonStart','exonEnd'], 
                how='left')
output1.to_csv(directory4, index=None)


# In[ ]:


# #Doesn't currently work
# directory5 ='/Users/meganholmes/Desktop/Documents/Rotations/Hertel_lab/Database_Expansion/Final_Join.xlsx'
# # writing to Excel
# datatoexcel = pd.ExcelWriter(directory5)
  
# # write DataFrame to excel
# output1.to_excel(datatoexcel)
  
# # save the excel
# datatoexcel.save()

