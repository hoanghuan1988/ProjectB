import pandas as pd
import numpy as np


df = pd.read_csv('PO_Forecast.csv')
d1 = df.copy()
listA = ['No.','CUSTOMER_GROUP','CusName','Model,''PartNo','Parname','GROUP_NAME','Surface','BeatName','DIAMETER1','DIAMETER2','THICKNESS','LENGTH','OperName']
ListB = df.columns
df.at[4,'No.']
