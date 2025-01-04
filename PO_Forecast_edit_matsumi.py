import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta

today = datetime.today()

m0 = today.strftime("%m/%Y_PCS")
m0x = today.strftime("%m/%Y_Wgt")
m0y = today.strftime("%Y.%m")

next_m1 = today + relativedelta(months=1)
next_m2 = today + relativedelta(months=2)
next_m3 = today + relativedelta(months=3)
next_m4 = today + relativedelta(months=4)
next_m5 = today + relativedelta(months=5)
next_m6 = today + relativedelta(months=6)
next_m7 = today + relativedelta(months=7)
next_m8 = today + relativedelta(months=8)
next_m9 = today + relativedelta(months=9)
next_m10 = today + relativedelta(months=10)
next_m11 = today + relativedelta(months=11)
next_m12 = today + relativedelta(months=12)

m1 = next_m1.strftime("%m/%Y_PCS")
m1x = next_m1.strftime("%m/%Y_Wgt")
m1y = next_m1.strftime("%Y.%m")

m2 = next_m2.strftime("%m/%Y_PCS")
m2x = next_m2.strftime("%m/%Y_Wgt")
m2y = next_m2.strftime("%Y.%m")

m3 = next_m3.strftime("%m/%Y_PCS")
m3x = next_m3.strftime("%m/%Y_Wgt")
m3y = next_m3.strftime("%Y.%m")

m4 = next_m4.strftime("%m/%Y_PCS")
m4x = next_m4.strftime("%m/%Y_Wgt")
m4y = next_m4.strftime("%Y.%m")

m5 = next_m5.strftime("%m/%Y_PCS")
m5x = next_m5.strftime("%m/%Y_Wgt")
m5y = next_m5.strftime("%Y.%m")

m6 = next_m6.strftime("%m/%Y_PCS")
m6x = next_m6.strftime("%m/%Y_Wgt")
m6y = next_m6.strftime("%Y.%m")

m7 = next_m7.strftime("%m/%Y_PCS")
m7x = next_m7.strftime("%m/%Y_Wgt")
m7y = next_m7.strftime("%Y.%m")

m8 = next_m8.strftime("%m/%Y_PCS")
m8x = next_m8.strftime("%m/%Y_Wgt")
m8y = next_m8.strftime("%Y.%m")

m9 = next_m9.strftime("%m/%Y_PCS")
m9x = next_m9.strftime("%m/%Y_Wgt")
m9y = next_m9.strftime("%Y.%m")

m10 = next_m10.strftime("%m/%Y_PCS")
m10x = next_m10.strftime("%m/%Y_Wgt")
m10y = next_m10.strftime("%Y.%m")

m11 = next_m11.strftime("%m/%Y_PCS")
m11x = next_m11.strftime("%m/%Y_Wgt")
m11y = next_m11.strftime("%Y.%m")

m12 = next_m12.strftime("%m/%Y_PCS")
m12x = next_m12.strftime("%m/%Y_Wgt")
m12y = next_m12.strftime("%Y.%m")

dk = pd.read_csv('PO_Forecast.csv')
df = dk[dk['GROUP_NAME'].notna()]
ListA = ['CUSTOMER_GROUP', 'CusName', 'Model', 'PartNo', 'PartName','GROUP_NAME', 'Surface', 'BeatName', 'DIAMETER1', 'DIAMETER2','THICKNESS', 'LENGTH', 'OperName']

#ListA1 = ListA .extend([m0,m0x])
ListA0 = ListA +[m0]+[m0x]
ListA1 = ListA +[m1]+[m1x]
ListA2 = ListA +[m2]+[m2x]
ListA3 = ListA +[m3]+[m3x]
ListA4 = ListA +[m4]+[m4x]
ListA5 = ListA +[m5]+[m5x]
ListA6 = ListA +[m6]+[m6x]
ListA7 = ListA +[m7]+[m7x]
ListA8 = ListA +[m8]+[m8x]
ListA9 = ListA +[m9]+[m9x]
ListA10 = ListA +[m10]+[m10x]
ListA11 = ListA +[m11]+[m11x]
ListA12 = ListA +[m12]+[m12x]

df0 = df.filter(ListA0)
A0 = (m0,m0x)
set_A0 = set(A0)
for x in set_A0:rename0 = df0.rename(columns={m0:'Quatity(PCS)',m0x:'Weight(Kg)'})

df1 = df.filter(ListA1)
A1 = (m1,m1x)
set_A1 = set(A1)
for x in set_A1:rename1 = df1.rename(columns={m1:'Quatity(PCS)',m1x:'Weight(Kg)'})

df2 = df.filter(ListA2)
A2 = (m2,m2x)
set_A2 = set(A2)
for x in set_A2:rename2 = df2.rename(columns={m2:'Quatity(PCS)',m2x:'Weight(Kg)'})

df3 = df.filter(ListA3)
A3 = (m3,m3x)
set_A3 = set(A3)
for x in set_A3:rename3 = df3.rename(columns={m3:'Quatity(PCS)',m3x:'Weight(Kg)'})

df4 = df.filter(ListA4)
A4 = (m4,m4x)
set_A4 = set(A4)
for x in set_A4:rename4 = df4.rename(columns={m4:'Quatity(PCS)',m4x:'Weight(Kg)'})

df5 = df.filter(ListA5)
A5 = (m5,m5x)
set_A5 = set(A5)
for x in set_A5:rename5 = df5.rename(columns={m5:'Quatity(PCS)',m5x:'Weight(Kg)'})

df6 = df.filter(ListA6)
A6 = (m6,m6x)
set_A6 = set(A6)
for x in set_A6:rename6 = df6.rename(columns={m6:'Quatity(PCS)',m6x:'Weight(Kg)'})

df7 = df.filter(ListA7)
A7 = (m7,m7x)
set_A7 = set(A7)
for x in set_A7:rename7 = df7.rename(columns={m7:'Quatity(PCS)',m7x:'Weight(Kg)'})

df8 = df.filter(ListA8)
A8 = (m8,m8x)
set_A8 = set(A8)
for x in set_A8:rename8 = df8.rename(columns={m8:'Quatity(PCS)',m8x:'Weight(Kg)'})

df9 = df.filter(ListA9)
A9 = (m1,m1x)
set_A9 = set(A1)
for x in set_A1:rename9 = df9.rename(columns={m9:'Quatity(PCS)',m9x:'Weight(Kg)'})

df10 = df.filter(ListA10)
A10 = (m10,m10x)
set_A10 = set(A10)
for x in set_A10:rename10 = df10.rename(columns={m10:'Quatity(PCS)',m10x:'Weight(Kg)'})

df11 = df.filter(ListA11)
A11 = (m11,m11x)
set_A11 = set(A11)
for x in set_A11:rename11 = df11.rename(columns={m11:'Quatity(PCS)',m11x:'Weight(Kg)'})

df12 = df.filter(ListA12)
A12 = (m12,m12x)
set_A12 = set(A12)
for x in set_A12:rename12 = df12.rename(columns={m12:'Quatity(PCS)',m12x:'Weight(Kg)'})

rename0['Month']=m0y
rename1['Month']=m1y
rename2['Month']=m2y
rename3['Month']=m3y
rename4['Month']=m4y
rename5['Month']=m5y
rename6['Month']=m6y
rename7['Month']=m7y
rename8['Month']=m8y
rename9['Month']=m9y
rename10['Month']=m10y
rename11['Month']=m11y
rename12['Month']=m12y
result = pd.concat([rename0,rename1,rename2,rename3,rename4,rename5,rename6,rename7,rename8,rename9,rename10,rename11])

result.to_excel('result.xlsx')

