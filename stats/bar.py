import matplotlib.pyplot as plt
import pandas as pd
import numpy

fortyk=pd.read_csv('40k.csv')
vector= list(fortyk.vector)
mnb=list(fortyk.mnb)
sgd=list(fortyk.sgd)
bnb=list(fortyk.bnb)

vec_avg=sum(vector)/len(vector)
sgd_avg=sum(sgd)/len(sgd)
mnb_avg=sum(mnb)/len(mnb)
bnb_avg=sum(bnb)/len(bnb)

x_ax=["Vector","mnb","sgd","bnb"]
y_ax=[vec_avg,mnb_avg,sgd_avg,bnb_avg]
plt.bar(x_ax,y_ax)
