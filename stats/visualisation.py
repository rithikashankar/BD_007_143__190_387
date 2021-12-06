import pandas as pd
from matplotlib import pyplot as plt

tenk_file = open("10k.txt", "r")#opening files in read mode
twentyk_file=open("20k.txt", "r")
fortyk_file=open("40k.txt", "r")

tenk=tenk_file.read() #reading files
twentyk=twentyk_file.read()
fortyk=fortyk_file.read()

tenk_list=tenk.split("\n") #using split to make it a list
twentyk_list=twentyk.split("\n")
fortyk_list=fortyk.split("\n")


del tenk_list[-1]; #removing extra null char
del twentyk_list[-1];
del fortyk_list[-1];

tenk_list=list(map(float, tenk_list)) #converting string to float
twentyk_list=list(map(float, twentyk_list))
fortyk_list=list(map(float, fortyk_list))

tenk_file.close()
twentyk_file.close()
fortyk_file.close()
 
tenk_avg=sum(tenk_list)/len(tenk_list) #avg of each list to plot on x axis
twentyk_avg=sum(twentyk_list)/len(twentyk_list)
fortyk_avg=sum(fortyk_list)/len(fortyk_list)

x_ax=[tenk_avg,twentyk_avg,fortyk_avg] #list of average time taken for 10k,20k,40k
y_ax=[10,20,40] #list of batchsizes

plt.plot(x_ax,y_ax) #plotting
plt.xlabel("Average Time")
plt.ylabel("Batch Size")


