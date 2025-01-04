import pandas as pd
import numpy as np
from numpy import nan as NA
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

def create_sql_engine():
    con_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=192.168.1.20\\MKSSQL2017;DATABASE=SUNSCO_HN;UID=sa;PWD=sa1234"
    con_url = URL.create("mssql+pyodbc",query={"odbc_connect":con_str})
    engine = create_engine(con_url)
    return engine

def get_stock_fur():
    engine = create_sql_engine()
    SQL = "exec sp_VIE060_LoadData '1','%','%','%' ,'20240401','20240430','20240301','20240331' ,'E','%','%','%','%'"
    SQL1 = "SELECT CUSTOMER_CODE,E_NAME FROM CUSTOMMF"
    #df = pd.read_sql(SQL,engine)
    #df.to_csv("Database/Stock_fur.csv" ,index = False)
    dk = pd.read_sql(SQL1,engine)
    dk.to_csv('Database/Cus.csv',index = False)

def get_stock_ima():
    engine = create_sql_engine()
    SQL = "SELECT A.ITEM_CODE,GROUP1, GROUP3, GROUP4_E as GROUP4, GROUP5_E as GROUP5, DIAMETER1 ,DIAMETER2,DIAMETER3,THICKNESS,THICKNESS2,WIDTH,WIDTH2,[LENGTH],COLOR_NAME_E as COLOR_NAME,PROCESSING_NAME_E as PROCESSING_NAME,CURRENT_STOCK,END_IN_DATE,WEIGHT,MEMO, A.LOT_NO,ORDER_NO,UNIT_ITEM, A.MEMO_ERR_STATUS, ITEM_STATUS = A.ITEM_STATUS,A.CUSTOMERCODE,A.manage_divi,A.OVT  FROM [fn_VIE010_ShowData_Customer]('%','%','%','%','%', '%', '%') A  ORDER BY A.manage_divi, A.CUSTOMERCODE, A.grade"
    df = pd.read_sql(SQL,engine)
    df.loc[df['COLOR_NAME']!= "IBC",'COLOR_NAME'] = "NBC"
    df['CUSTOMERCODE'] =df['CUSTOMERCODE'].str.replace('c','C')
    df['CUSTOMERCODE'] =df['CUSTOMERCODE'].str.replace('s','S')
    df.loc[df['LENGTH'] == 0,'ITEM_STATUS'] = df['CUSTOMERCODE'].astype(str)+df['ITEM_CODE'].astype(str)+df['THICKNESS2'].astype(str)
    df.loc[df['LENGTH'] != 0,'ITEM_STATUS'] = df['CUSTOMERCODE'].astype(str)+df['ITEM_CODE'].astype(str)+df['LENGTH'].astype(str)+df['COLOR_NAME'].astype(str)
    df.loc[df['LENGTH'] == 0,'MEMO_ERR_STATUS'] = df['ITEM_CODE'].astype(str)+df['THICKNESS2'].astype(str)
    df.loc[df['LENGTH'] != 0,'MEMO_ERR_STATUS'] = df['ITEM_CODE'].astype(str)+df['LENGTH'].astype(str)
    #df['ITEM_STATUS'] = df['CUSTOMERCODE'].astype(str)+df['CUSTOMERCODE'].astype(str)+df['ITEM_CODE'].astype(str)+df['THICKNESS'].astype(str)+df['THICKNESS2'].astype(str)+df['WIDTH'].astype(str)+df['LENGTH'].astype(str)+df['COLOR_NAME'].astype(str)
    df.loc[df['LENGTH'] != 0,'MEMO']  = df['WEIGHT']*df['CURRENT_STOCK']*df['LENGTH']/1000
    df.loc[df['LENGTH'] == 0,'GROUP4'] = df['GROUP5']
    df['split_name'] = df['ORDER_NO'].str.split('-')
    df['div'] = df['split_name'].str.get(0)
    df['pro'] = df['split_name'].str.get(1)  
    df.to_csv('Database/Stock_ima.csv',index = False)

def get_stock_ending():
    engine = create_sql_engine()
    SQL = "exec [proc_VIE050_ShowData_Last] 'DLVWH,HN001,HN001.1,HN002,HN002.1,HN003,HN004,HN005,HN006,HN007,HN008,HN009,IPWH,OOPWH','%','%','20240601',1"
    df = pd.read_sql(SQL,engine)
    last_stock = df.drop(['item_code_suffix','group15','management_division_name_v','management_division_name_e','management_division_name_j','place_name_v','place_name_j','surface_name_v','surface_name_j','coating_name_v','coating_name_j','color_name_v','color_name_j','processing_name_v','processing_name_j'],axis=1)
    last_stock.to_csv("Database/Last_stock.csv" ,index = False)

def get_po():
    engine = create_sql_engine()
    #SQL = "exec sp_CUS040_Showdata_Step2 '20241201','%','%','%','%','%','%'"
    SQL = "exec sp_CUS040_ShowdataSummary_Step2 '20250101','%','%','%','%','%','%'"
    SQL1 = "exec sp_CUS040_Showdata_Step2 '20250101','%','%','%','%','%','%'"
    dk = pd.read_sql(SQL,engine)
    dj = pd.read_sql(SQL1,engine)
    #dk = dk.round(0)
    df = dk.round({'01/2025_Wgt':0,'02/2025_Wgt':0,'03/2025_Wgt':0,'04/2025_Wgt':0,'05/2025_Wgt':0,'06/2025_Wgt':0,'07/2025_Wgt':0,'08/2025_Wgt':0,'09/2025_Wgt':0,'10/2025_Wgt':0,'11/2025_Wgt':0,'12/2025_Wgt':0,'1/2026_Wgt':0,'2/2026_Wgt':0})
    #lst = list(dk.columns)
    dj.loc[dj['CUSTOMER_CODE'].notna(),'GROUP_CODE'] = dj['CUSTOMER_CODE'].astype(str)+dj['ITEM_CODE'].astype(str)+dj['LENGTH'].astype(str)+dj['COLOR_NAME_E'].astype(str)
    df.to_csv('Report/5_PO_Forecast.csv',index = False)
    dj.to_csv("Database/PO_Forecast.csv" ,index = False)
  

def get_BOM():
    #from plyer import notificationd
    con_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=192.168.1.20\\MKSSQL2017;DATABASE=SUNSCO_HN;UID=sa;PWD=sa1234"
    con_url = URL.create("mssql+pyodbc",query={"odbc_connect":con_str})
    engine = create_engine(con_url)
    # Stock
    SQL2 = "SELECT ParentItemCode,ParentThickness2,ParentLength,ChildItemCode,ChildThickness2,ChildLength,ParentQty,ProcessCode,Main_Material,INPUT_DATE From SPREADMF"
    SQL = "SELECT ITEM_CODE,ITEM_NAME_V,ITEM_GROUP1,ITEM_GROUP2,ITEM_GROUP3,ITEM_GROUP4,ITEM_GROUP5,GROUP15,DIAMETER1,DIAMETER2,THICKNESS,WIDTH,INV_DV,isSquare,DIAMETER_Origin,INPUT_DATE,MODIFY_DATE FROM PRODUCTMF"
    SQL3 ="SELECT GROUP_CLASSIFY,GROUP_CODE_1,GROUP_CODE_2,GROUP_CODE_3,GROUP_CODE_4,GROUP_CODE_5,SHORT_NAME,GROUP_NAME_E,INPUT_DATE,MODIFY_DATE,INV_DV FROM GROUPMF"
    SQL4 =  "exec sp_Load_CUS025 '%','%','%','4','E'"
    BOM = pd.read_sql(SQL2,engine)
    BOM.to_csv("Database/BOM.csv",index = False)
    Item = pd.read_sql(SQL,engine)
    Item.to_csv("Database/Itemcode.csv",index = False)
    Name = pd.read_sql(SQL3,engine)
    Name_f = Name[(Name['INV_DV'] != "*")]
    Delivery_item = pd.read_sql(SQL4,engine)
    Delivery_item['ITEM_STATUS'] = Delivery_item['CUSTOMER_CODE'].astype(str)+Delivery_item['ITEM_CODE'].astype(str)+Delivery_item['LENGTH'].astype(str)+Delivery_item['ColorName'].astype(str)
    Delivery_item.to_csv("Database/Delivery_item.csv",index = False)
    Name_f.to_csv("Database/Code_Name.csv",index = False)

def read_fur_stock():
    stock_read = pd.read_csv('Database/Stock_fur.csv')
    #cus = pd.read_csv('Database/Customer.csv')
    #dk = pd.read_excel('Report/Report_cuong.xlsx',index_col=0)
    # Chinh sua du lieu stock
    stock_filter = stock_read[(stock_read["DIAMETER1"] !=0) & (stock_read["future_Stock"] >0)]
    stock = stock_filter.drop(['ITEM_CODE','GROUP_CODE','TotalLast_StockOut','TotalStockOut','HisStockOut','StockOut','TotalCurrentStock','TotalPln','PlnQty','ikey'],axis=1)
    #cus1 = cus.dropna()
    #cusname = pd.merge(stock,cus1,how="left",on=["CUSTOMER_CODE"])
    #new_stock = cusname.drop(['CUSTOMER_CODE'],axis=1)
    # Chinh sua du lieu Mr.Cuong
    #dk_s = dk.loc[0:,['CUSTOMER_NAME','Material_LP','DIAMETER1','DIAMETER2','THICKNESS','Group_LP','BeatCut']]
    #long = dk_s.dropna()
    #short_p = dk.loc[0:,['CUSTOMER_NAME','GROUP_NAME','DIAMETER1','DIAMETER2','THICKNESS','LENGTH','BeatCut']]
    #long_p = long.rename(columns={'Material_LP':'GROUP_NAME','Group_LP':'LENGTH'})
    #stock = pd.DataFrame(dk)
    #longp = pd.DataFrame(df3)
    #result = pd.merge(left,right,how="left",on=["GROUP_NAME","DIAMETER1","DIAMETER2","THICKNESS","LENGTH","BeatCut"])
    
def BOM_Compare():
    r_bom = pd.read_csv('Database/BOM.csv')
    r_delivery = pd.read_csv('Database/Delivery_item.csv')
    #r_item = pd.read_csv('Database/Itemcode.csv')
    #r_name = pd.read_csv('Database/Code_Name.csv')
    #item_name = pd.merge(r_item,r_name,how="left",on=["GROUP_CODE_1","GROUP_CODE_2","GROUP_CODE_3","GROUP_CODE_4","GROUP_CODE_5"])
    #name1 = r_name.loc[0:,['GROUP_CODE_1','GROUP_CODE_2','GROUP_CODE_3','GROUP_CODE_4','GROUP_CODE_5','SHORT_NAME']]
    #xoa item user deleted.
    #item_fix = r_item[(r_item['INV_DV'] != "*")&(r_item['WIDTH'] >0 )]
    #sua index de mapping
    #r_name_fix = r_name.rename(columns={'GROUP_CODE_1':'ITEM_GROUP1','GROUP_CODE_2':'ITEM_GROUP2','GROUP_CODE_3':'ITEM_GROUP3','GROUP_CODE_4':'ITEM_GROUP4','GROUP_CODE_5':'ITEM_GROUP5'})
    #item_name = pd.merge(item_fix,r_name_fix,how="left",on=["ITEM_GROUP1","ITEM_GROUP2","ITEM_GROUP3","ITEM_GROUP4"])
    #item = item_name.loc[0:,['ITEM_CODE','DIAMETER1','DIAMETER2','THICKNESS','WIDTH','SHORT_NAME']]
    #item_filter = item[(item['SHORT_NAME'] != "0")&(item['SHORT_NAME'] != "00")]
    #item_filter.to_csv('Database/Coil_Name.csv')
    # Bien doi Delivery_Item
    #Delivery = r_delivery.loc[0:,['CUSTOMER_CODE','CUSTOMER_GROUP','ITEM_CODE','Model','PartNo','PartName','MaterialName','DIAMETER1','DIAMETER2','THICKNESS','LENGTH','ColorName','ProcessName','SurfaceName','INV_DV','HideInCUS030']]
    #Delivery_filter = Delivery[(Delivery['INV_DV'] != "*")&(Delivery['HideInCUS030'] != 1)]
    #Delivery_filter.to_csv('Database/Pipe_Name.csv')
    r_bom.loc[r_bom['ParentLength'] == 0,'Parent_Code'] = r_bom['ParentItemCode'].astype(str)+r_bom['ParentThickness2'].astype(str)
    r_bom.loc[r_bom['ParentLength'] != 0,'Parent_Code'] = r_bom['ParentItemCode'].astype(str)+r_bom['ParentLength'].astype(str)
    r_bom.loc[r_bom['ChildLength'] == 0,'Child_Code'] = r_bom['ChildItemCode'].astype(str)+r_bom['ChildThickness2'].astype(str)
    r_bom.loc[r_bom['ChildLength'] != 0,'Child_Code'] = r_bom['ChildItemCode'].astype(str)+r_bom['ChildLength'].astype(str)
    #r_bom.to_csv('Database/BOM_fix.csv')
    f_bom = r_bom.rename(columns={'Parent_Code':'Child_Code','Child_Code':'Parent_Code'})
    df = f_bom.drop(['ParentItemCode','ParentThickness2','ParentLength','ChildItemCode','ChildThickness2','ChildLength','INPUT_DATE'],axis=1)
    #df.to_csv('Database/BOM_fix2.csv')
    #df.to_csv('Database/BOM_fix3.csv')
    lv1 = r_bom.merge(df,how='left',on='Child_Code')
    #lv1.to_csv('Database/BOM_fix3.csv')
    f_lv1 = lv1.rename(columns={'ParentQty_x':'Qty_0','ProcessCode_x':'Pro_0','Main_Material_x':'Main_0','ParentQty_y':'Qty_1','ProcessCode_y':'Pro_1','Main_Material_y':'Main_1'})
    lv2 = f_lv1.rename(columns={'Parent_Code_y':'Parent_Code'})
    lv3 = lv2.merge(r_bom,how='left',on='Parent_Code')
    #lv3.to_csv('Database/BOM_fix3.csv')
    lvk = lv3.rename(columns={'ParentQty':'Qty_2','ProcessCode':'Pro_2','Main_Material':'Main_2','Parent_Code_x':'Parent1','Main_Material_x':'Main_Material1','ParentQty_x':'ParentQty'})
    #lvk = lv3.drop(['Parent_Code_x', 'ProcessCode_x', 'Main_Material_x', 'ParentQty_x'],axis=1)
    dk = df.rename(columns={'Child_Code':'Child_Code_y'})
    lv4 = lvk.merge(dk,how='left',on='Child_Code_y') 
    lvi = lv4.rename(columns={'Parent_Code_y':'lv5','ParentQty':'Qty_3','ProcessCode':'Pro_3','Main_Material':'Main_3'})
    dt = df.rename(columns={'Child_Code':'lv5'})
    lv5 = lvi.merge(dt,how='left',on='lv5')
    lve = lv5.rename(columns={'ProcessCode_x':'ProcessCode2','Main_Material_x':'Main_Material2','ParentQty_x':'ParentQty2','Parent_Code':'lv6','ParentQty':'Qty_4','ProcessCode':'Pro_4','Main_Material':"Main_4"})
    de = df.rename(columns={'Child_Code':'lv6'})
    lv6 = lve.merge(de,how='left',on='lv6')
    end_bom = lv6.drop(['ParentItemCode_x','ChildItemCode_x','INPUT_DATE_x','INPUT_DATE_y','ParentThickness2_x','ParentLength_x','ChildThickness2_x','ChildLength_x','ParentItemCode_y','ParentThickness2_y','ParentLength_y','ChildItemCode_y','ChildThickness2_y','ChildLength_y'],axis=1)
    f_end_bom = end_bom.rename(columns={'ParentQty_y':'Qty_y','ParentQty':'Qty_5','ProcessCode':'Pro_5','Main_Material':'Main_5','Parent1':'Code_0','Child_Code_x':'Code_1','Parent_Code_x':'Code_2','Child_Code_y':'Code_3','lv5':'Code_4','lv6':'Code_5','Parent_Code':'Code_6'})
    Name_bom = f_end_bom[(end_bom['Pro_0'] != "PS")]
    Ending = Name_bom.loc[0:,['Code_0','Code_1','Code_2','Code_3','Code_4','Code_5','Code_6','Qty_0','Pro_0','Main_0','Qty_1','Pro_1','Main_1','Qty_2','Pro_2','Main_2','Qty_3','Pro_3','Main_3','Qty_4','Pro_4','Main_4','Qty_5','Pro_5','Main_5']]
    tab3 = Ending[(Ending['Pro_1'] != "PS")|(Ending['Main_1'] == 1)]
    #tab3.to_csv('Database/BOM_fix3.csv')
    tab4 = tab3[(tab3['Pro_2'] != "PS")|(tab3['Main_2'] == 1)]
    tab5 = tab4[(tab4['Pro_3'] != "PS")|(tab4['Main_3'] == 1)]
    #tab6 = tab5[(tab5['Pro_4'] != "PS")|(tab5['Main_4'] == 1)]
    tab7 = tab5[(tab5['Pro_4'] != "PS")|(tab5['Main_4'] == 1)]
    #Phan tach cong doan CUT/PIPE/SLIT/COIL
    tab7.loc[tab7['Pro_0'] != 'CO','CUT'] =  tab7['Code_0']
    tab7.loc[tab7['Qty_5'] != 1,'RATE'] =  tab7['Qty_5']
    tab7.loc[tab7['Qty_4'] != 1,'RATE'] =  tab7['Qty_4']
    tab7.loc[tab7['Qty_3'] != 1,'RATE'] =  tab7['Qty_3']
    tab7.loc[tab7['Qty_2'] != 1,'RATE'] =  tab7['Qty_2']
    tab7.loc[tab7['Qty_1'] != 1,'RATE'] =  tab7['Qty_1']
    tab7.loc[tab7['Qty_0'] != 1,'RATE'] =  tab7['Qty_0']
    #tab7.loc[(tab7['Qty_1'] != 1) | (tab7['Pro_1'].isna())|tab7['Qty_0'] != 1,'RATE'] =  tab7['Qty_1']
    tab7.loc[tab7['Pro_0'] == 'CO','PIPE'] =  tab7['Code_0']
    tab7.loc[tab7['Pro_1'] == 'CO','PIPE'] =  tab7['Code_1']
    tab7.loc[tab7['Pro_2'] == 'CO','PIPE'] =  tab7['Code_2']
    tab7.loc[tab7['Pro_3'] == 'CO','PIPE'] =  tab7['Code_3']
    tab7.loc[tab7['Pro_4'] == 'CO','PIPE'] =  tab7['Code_4']
    tab7.loc[tab7['Pro_5'] == 'CO','PIPE'] =  tab7['Code_5']
    tab7.loc[tab7['Pro_1'] == 'PS','SLIT'] =  tab7['Code_1']
    tab7.loc[tab7['Pro_1'] == 'PS','COIL'] =  tab7['Code_2']
    tab7.loc[tab7['Pro_2'] == 'PS','SLIT'] =  tab7['Code_2']
    tab7.loc[tab7['Pro_2'] == 'PS','COIL'] =  tab7['Code_3']
    tab7.loc[tab7['Pro_3'] == 'PS','SLIT'] =  tab7['Code_3']
    tab7.loc[tab7['Pro_3'] == 'PS','COIL'] =  tab7['Code_4']
    tab7.loc[tab7['Pro_4'] == 'PS','SLIT'] =  tab7['Code_4']
    tab7.loc[tab7['Pro_4'] == 'PS','COIL'] =  tab7['Code_5']
    tab7.loc[tab7['Pro_5'] == 'PS','SLIT'] =  tab7['Code_5']
    tab7.loc[tab7['Pro_5'] == 'PS','COIL'] =  tab7['Code_6']
    tab7.loc[tab7['PIPE'].isna(),'PIPE'] =  tab7['Code_1']
    tab7.to_csv('Database/BOM_map.csv')
    #Delivery_filter = Delivery[(Delivery['INV_DV'] != "*")&(Delivery['HideInCUS030'] != 1)]
    #Delivery_item['ITEM_STATUS'] = Delivery_item['CUSTOMER_CODE'].astype(str)+Delivery_item['ITEM_CODE'].astype(str)+Delivery_item['LENGTH'].astype(str)+Delivery_item['ColorName'].astype(str)

def data_analyst():
    r_bom = pd.read_csv('Database/BOM.csv')
    #r_cus = pd.read_csv('Database/Pipe_Name.csv')
    r_delivery = pd.read_csv('Database/Delivery_item.csv')
    r_stock = pd.read_csv('Database/Stock_ima.csv')
    Bom1 = r_bom.loc[0:,['ParentItemCode','ParentThickness2','ParentLength','ChildItemCode','ChildThickness2','ChildLength','ParentQty','ProcessCode','Main_Material']]
    Bom1_rename = Bom1.rename(columns={'ParentItemCode':'ITEM_CODE','ParentLength':'LENGTH'})
    Bom2 = r_bom.loc[0:,['ParentItemCode','ParentThickness2','ParentLength','ChildItemCode','ChildThickness2','ChildLength','ParentQty','ProcessCode','Main_Material']]
    Bom2_rename = Bom2.rename(columns={'ParentItemCode':'pipe_c','ParentThickness2':'pipe_t','ParentLength':'pipe_l'})
    #lv1 = pd.merge(r_cus,Bom1_rename,how="inner",on=["ITEM_CODE","LENGTH"])
    #sort_lv1 = lv1.sort_values(by=['CUSTOMER_CODE','DIAMETER1','THICKNESS','LENGTH'])
    #lv1_filter = sort_lv1[(sort_lv1['ChildLength'] != 0)&(sort_lv1['ProcessCode'] != "TO")]
    #lv1_filter_rename = lv1_filter.rename(columns={'ChildItemCode':'pipe_c','ChildThickness2':'pipe_t','ChildLength':'pipe_l'})
    #lv1_filter1 = sort_lv1[(sort_lv1['ChildThickness2'] != 0)]
    #lv1_filter1_rename =lv1_filter1.rename(columns={'ChildItemCode':'pipe_c','ChildThickness2':'pipe_t','ChildLength':'pipe_l'})
    #lv1_filter2 = sort_lv1[(sort_lv1['ProcessCode'] == "TO")]
    #lv1_filter2_rename =lv1_filter2.rename(columns={'ChildItemCode':'pipe_c','ChildThickness2':'pipe_t','ChildLength':'pipe_l'})
    #lv2 = pd.merge(lv1_filter_rename,Bom2_rename,how="inner",on=["pipe_c","pipe_t","pipe_l"])
    #lv2.to_excel('Route/cut.xlsx')
    #lv2_1 = pd.merge(lv1_filter1_rename,Bom2_rename,how="inner",on=["pipe_c","pipe_t","pipe_l"])
    #lv2_1_filter = lv2_1[(lv2_1['Main_Material_y'] == 1)]
    #lv2_1_filter.to_excel('Route/piping.xlsx')
    #lv2_2= pd.merge(lv1_filter2_rename,Bom2_rename,how="inner",on=["pipe_c","pipe_t","pipe_l"])
    #lv2_2.to_excel('Route/chamfer.xlsx')
    gr_stock = r_stock.groupby(['ITEM_STATUS','MEMO_ERR_STATUS','ITEM_CODE','CUSTOMERCODE','GROUP4','DIAMETER1','DIAMETER2','THICKNESS','THICKNESS2','WIDTH','LENGTH','COLOR_NAME'])
    df = gr_stock.agg({'CURRENT_STOCK':np.sum,'MEMO':np.sum})
    df.loc[df['MEMO'] == 0,'MEMO'] =  df['CURRENT_STOCK']
    #df.to_csv('Database/report_stock_ima.csv')
    lv3_deliver_stock = pd.merge(df,r_delivery,how="left",on=["ITEM_STATUS"])
    lv3_deliver_stock.to_csv('Database/group_stock_delivery.csv')

def stock_analyst():
    stock_delivery = pd.read_csv('Database/group_stock_delivery.csv')
    r_bom = pd.read_csv('Database/BOM_map.csv')
    r_stock = pd.read_csv('Database/Stock_ima.csv')
    r_cus = pd.read_csv('Database/Cus.csv')
    stock = stock_delivery.drop_duplicates(subset=['ITEM_STATUS'])
    tab1 = stock.loc[0:,['ITEM_STATUS','ITEM_CODE','CUSTOMER_CODE','CUSTOMER_GROUP','DLV_PLACE','Model','PartNo','PartName','MaterialName','DIAMETER1','DIAMETER2','THICKNESS','LENGTH','ColorName','ProcessName','CURRENT_STOCK','MEMO']]
    tab3 = tab1[(tab1['CURRENT_STOCK'] > 0)&(tab1['CUSTOMER_CODE'].notna())]
    tab4 = tab1[tab1['CUSTOMER_CODE'].isna()]
    tab3.loc[0:,['CUT']] =  tab3['ITEM_CODE'].astype(str)+tab3['LENGTH'].astype(str)
    BOM = r_bom.loc[0:,['CUT','PIPE','SLIT','COIL']]
    BOM_x = BOM.drop_duplicates(subset=['PIPE'])
    #BOM.to_csv('Database/BOM_x.csv')
    tab5 = tab4.merge(r_stock,how="left", on=["ITEM_STATUS"])
    d_5 = tab5.drop_duplicates(subset=['ITEM_STATUS'])
    #d_5.to_csv('Database/d_5.csv')
    f_5 = d_5.loc[0:,['MEMO_ERR_STATUS','ITEM_CODE_y','CUSTOMERCODE','GROUP4','GROUP5','DIAMETER1_y','DIAMETER2_y','DIAMETER3','THICKNESS_y','THICKNESS2','WIDTH','WIDTH2','LENGTH_y','COLOR_NAME','CURRENT_STOCK_x','MEMO_x']]
    re_f5 = f_5.rename(columns={'MEMO_ERR_STATUS':'CUT'})
    mer_f5 = re_f5.merge(r_bom,how="left",on=["CUT"])
    mer_f5.loc[mer_f5['PIPE'].notna(),'CUT'] =  mer_f5['PIPE']
    mer_f5.loc[mer_f5['PIPE'].notna(),'Unnamed: 0'] =  mer_f5['CURRENT_STOCK_x']/mer_f5['RATE']
    nex_f5 = mer_f5.loc[0:,['CUT','ITEM_CODE_y','CUSTOMERCODE','GROUP4','GROUP5','DIAMETER1_y','DIAMETER2_y','DIAMETER3','THICKNESS_y','THICKNESS2','WIDTH','WIDTH2','LENGTH_y','COLOR_NAME','CURRENT_STOCK_x','MEMO_x','Unnamed: 0']]
    Semi_Coil = nex_f5[(nex_f5['DIAMETER1_y'] == 0)]
    Semi_Pipe = nex_f5[(nex_f5['DIAMETER1_y'] != 0)]
    Semi_Pipe.loc[Semi_Pipe['Unnamed: 0'].isna(),'Unnamed: 0'] = Semi_Pipe['CURRENT_STOCK_x']
    re_Semi_Pipe = Semi_Pipe.rename(columns={'CUT':'PIPE','Unnamed: 0':'Stock_LP'})
    Group_Semi_Pipe =re_Semi_Pipe.groupby(['PIPE'])
    Stock = Group_Semi_Pipe.agg({'Stock_LP':np.sum,'MEMO_x':np.sum})
    f_semi_scoil = Semi_Coil[(Semi_Coil['WIDTH'] < 800)]
    Re_semi_slit = f_semi_scoil.rename(columns={'CUT':'SLIT','CURRENT_STOCK_x':'Slit_Stock','GROUP4':'Slit_Name','THICKNESS2':'T_Slit','WIDTH':'Slit_W'})
    l_slit = Re_semi_slit.loc[0:,['SLIT','Slit_Name','T_Slit','Slit_W','Slit_Stock']]
    f_semi_coil = Semi_Coil[(Semi_Coil['WIDTH'] > 800)]
    Re_semi_coil = f_semi_coil.rename(columns={'CUT':'COIL','CURRENT_STOCK_x':'Coil_Stock','GROUP4':'Coil_Name','THICKNESS2':'C_Slit','WIDTH':'Coil_W'})
    l_coil = Re_semi_coil.loc[0:,['COIL','Coil_Name','C_Slit','Coil_W','Coil_Stock']]                         
    #Stock.to_csv('Database/test.csv')
    #Semi_Coil.to_csv('Database/Semi_Coil.csv')
    #re_Semi_Pipe.to_csv('Database/Semi_Pipe.csv')
    lv1 = tab3.merge(BOM,how='left',on='CUT')
    Good_sales = lv1.drop_duplicates(subset=['ITEM_STATUS'])
    Good_sales .loc[Good_sales ['PIPE'].isna(),'PIPE'] = Good_sales ['CUT']

    Mapping_stock = Good_sales.merge(Stock,how='outer',on='PIPE')
    M1 = Mapping_stock.merge(BOM_x,how='left',on='PIPE')
    Result_Map_Stock = M1.loc[0:,['ITEM_CODE','CUSTOMER_CODE','CUSTOMER_GROUP',	'DLV_PLACE'	,'Model'	,'PartNo',	'PartName'	,'MaterialName'	,'DIAMETER1'	,'DIAMETER2'	,'THICKNESS'	,'LENGTH'	,'ColorName'	,'ProcessName'	,'CURRENT_STOCK','MEMO','PIPE','Stock_LP','MEMO_x','SLIT_y','COIL_y']]
    Rename_result = Result_Map_Stock.rename(columns={'MEMO_x':'Weight_Lp','SLIT_y':'SLIT','COIL_y':'COIL'})
    Mapping_semi_pipe = Rename_result.merge(Stock,how='left',on='PIPE')
    Mapping_slit = Mapping_semi_pipe.merge(l_slit,how='outer',on='SLIT')
    
    Mapping_semi_pipe_2 =Mapping_slit.merge(re_Semi_Pipe,how='left',on='PIPE')
    #select_column = Mapping_semi_pipe_2.loc[0:,['CUSTOMER_CODE'	,'CUSTOMER_GROUP',	'MaterialName',	'DIAMETER1',	'DIAMETER2',	'THICKNESS',	'LENGTH'	,'ColorName',	'CURRENT_STOCK',	'MEMO', 'CUSTOMERCODE',	'GROUP4'	,'LENGTH_y'	,'COLOR_NAME', 'Stock_LP_x'	,'Weight_Lp', 'Slit_Name',	'T_Slit',	'Slit_W','Slit_Stock']]
    Mapping_coil = Mapping_semi_pipe_2.merge(l_coil,how='left',on='COIL')
    incus = Mapping_coil.merge(r_cus,how='left',on='CUSTOMER_CODE')
    rename_incus =incus.rename(columns={'E_NAME':'Cus1','CUSTOMER_CODE':'CUSTOMER_CODE1','CUSTOMERCODE':'CUSTOMER_CODE'})
    m_incus = rename_incus.loc[0:,['ITEM_CODE','CUSTOMER_CODE1','Cus1',	'CUSTOMER_GROUP',	'DLV_PLACE',	'Model'	,'PartNo',	'PartName',	'MaterialName',	'DIAMETER1',	'DIAMETER2',	'THICKNESS',	'LENGTH',	'ColorName',	'ProcessName',	'CURRENT_STOCK',	'MEMO',	'PIPE',	'Stock_LP_x',	'Weight_Lp',	'SLIT','Slit_Name',	'T_Slit',	'Slit_W'	,'Slit_Stock'	,'COIL',	'Stock_LP_y',	'MEMO_x_x',	'ITEM_CODE_y',	'CUSTOMER_CODE',	'GROUP4',	'GROUP5',	'DIAMETER1_y',	'DIAMETER2_y',	'THICKNESS_y',		'LENGTH_y',	'COLOR_NAME',	'CURRENT_STOCK_x',	'MEMO_x_y',	'Stock_LP',	'Coil_Name',	'C_Slit',	'Coil_W',	'Coil_Stock']]
    m1_incus = m_incus.merge(r_cus,how='left',on='CUSTOMER_CODE')
    m2_incus = m1_incus.loc[0:,['ITEM_CODE','CUSTOMER_CODE1','Cus1',	'CUSTOMER_GROUP',	'DLV_PLACE',	'Model'	,'PartNo',	'PartName',	'MaterialName',	'DIAMETER1',	'DIAMETER2',	'THICKNESS',	'LENGTH',	'ColorName',	'ProcessName',	'CURRENT_STOCK',	'MEMO',	'PIPE',	'Stock_LP_x',	'Weight_Lp',	'SLIT','Slit_Name',	'T_Slit',	'Slit_W'	,'Slit_Stock'	,'COIL',	'Stock_LP_y',	'MEMO_x_x',	'ITEM_CODE_y',	'E_NAME',	'GROUP4',	'GROUP5',	'DIAMETER1_y',	'DIAMETER2_y',	'THICKNESS_y',		'LENGTH_y',	'COLOR_NAME',	'CURRENT_STOCK_x',	'MEMO_x_y',	'Stock_LP',	'Coil_Name',	'C_Slit',	'Coil_W',	'Coil_Stock']]
    m2_incus.to_csv('Database/Good_sales.csv')

def stock_expried():
    expired_stock = pd.read_csv('Database/Stock_ima.csv')
    Bom_map2 = pd.read_csv('Database/BOM_map2.csv')
    bomx = Bom_map2.filter(['SLIT','DIAMETER1','DIAMETER2'])
    bomx_rename = bomx.rename(columns={'SLIT':'MEMO_ERR_STATUS','DIAMETER1':'D1','DIAMETER2':'D2'})
    tab1 = expired_stock[(expired_stock['pro'] == "PS")&(expired_stock['OVT'] >= "6T")]
    tab2 = expired_stock[(expired_stock['pro'] == "PS")&(expired_stock['OVT'] == "12T")]
    #frames = [tab1,tab2]
    result = pd.concat([tab1,tab2])
    #result.to_csv('Report/s_coil_expried.csv')
    tab3 = expired_stock[(expired_stock['div'] == "POR")&(expired_stock['OVT'] >= "6T")]
    tab4 = expired_stock[(expired_stock['div'] == "POR")&(expired_stock['OVT'] == "12T")]
    #frames2 = [tab3,tab4]
    result2 = pd.concat([tab3,tab4])
    i = result2[(result2.GROUP1 == 2 )].index
    result3= result2.drop(i)
    k = pd.concat([result,result3])
    
    k.loc[k['ITEM_STATUS'].notna(),'Group_code'] =  k['MEMO_ERR_STATUS'].astype(str)+k['OVT'].astype(str)
    s = k.groupby("Group_code").sum(numeric_only=True)
    #k['END_IN_DATE'] = pd.to_datetime(k['END_IN_DATE'])
    k['END_IN_DATE']= pd.to_datetime(k['END_IN_DATE'])
    l= k.groupby("Group_code").min()
    mer = l.merge(s,how='right',on='Group_code')
    #mer.to_csv('Database/Test.csv')
    re = mer.loc[:, ["GROUP3", "GROUP4","DIAMETER1_x","DIAMETER2_x","WIDTH_x","THICKNESS_x","THICKNESS2_x","LENGTH_x","CUSTOMERCODE","CURRENT_STOCK_y","MEMO_x","OVT","MEMO_ERR_STATUS","manage_divi_x","div","END_IN_DATE"]]
    #re.to_csv('Database/Test.csv')
    #re2 = re.rename({'MEMO_x': 'MEMO_y'}, axis=1)
    fix_re = re.merge(bomx_rename,how='left',on='MEMO_ERR_STATUS')
    fix_re.loc[fix_re['D1'].notna() , 'DIAMETER1_x'] = fix_re['D1']
    fix_re.loc[fix_re['D2']>0 , 'DIAMETER2_x'] = fix_re['D2']
    fix_re.loc[fix_re['MEMO_ERR_STATUS'].notna(),'MEMO_ERR_STATUS_2'] = fix_re['MEMO_ERR_STATUS'].astype(str) + fix_re['OVT'].astype(str) 
    fix2_re = fix_re.drop_duplicates(subset=['MEMO_ERR_STATUS_2'])
    #fix_re.to_csv('Database/Test.csv')
    s_re= fix2_re.sort_values(by=['div', 'GROUP4','THICKNESS2_x','WIDTH_x'],ascending=False)

    #s_re.to_csv('Database/Test.csv')
    s_re['div'].replace('POR','Coil',inplace=True)
    s_re.loc[s_re['WIDTH_x'] < 800,'div'] = "Slit"
    s_re.loc[s_re['div']!="Coil",'div'] = "Slit"
    s_re.loc[s_re['div']=="Coil",'manage_divi_x'] =1
    s_re.loc[s_re['div']=="Slit",'manage_divi_x'] =2
    s_re = s_re.sort_values(by=['manage_divi_x','GROUP3','GROUP4','WIDTH_x','THICKNESS2_x'])
    s_re.to_csv('Database/coil&slit_expried.csv')

def pipe_expried():
    pipe_ex = pd.read_csv('Database/Stock_ima.csv')
    coil_slit = pd.read_csv('Database/coil&slit_expried.csv')
    cus = pd.read_csv('Database/Cus.csv')
    tab1 = pipe_ex[(pipe_ex['GROUP1']==2)&(pipe_ex['OVT']>="3T")]
    tab2 = pipe_ex[(pipe_ex['GROUP1']==2)&(pipe_ex['OVT']=="12T")]
    result = pd.concat([tab1,tab2])
    i = result[(result.LENGTH >=3000)].index
    f = result[(result.LENGTH <3000)].index
    k = result.drop(i)
    j = result.drop(f)
    #j.to_csv('Report/test.csv')
    k.loc[k['ITEM_STATUS'].notna(),'Group_code'] =  k['ITEM_STATUS'].astype(str)+k['OVT'].astype(str)
    j.loc[j['ITEM_STATUS'].notna(),'Group_code'] =  j['ITEM_STATUS'].astype(str)+j['OVT'].astype(str)
    s = k.groupby("Group_code").sum(numeric_only=True)
    k['END_IN_DATE'] = pd.to_datetime(k['END_IN_DATE'])
    l= k.groupby("Group_code").min()
    mer = l.merge(s,how='left',on='Group_code')
    re = mer.loc[:, ["GROUP3", "GROUP4","DIAMETER1_x","DIAMETER2_x","WIDTH_x","THICKNESS_x","THICKNESS2_x","LENGTH_x","COLOR_NAME","CUSTOMERCODE","CURRENT_STOCK_y","MEMO_y","OVT","manage_divi_x","div","END_IN_DATE"]]
    s_re = re.sort_values(by=['GROUP4','DIAMETER1_x','THICKNESS_x','LENGTH_x'])
    s_re.loc[s_re['div'].notna(),'div'] = "Cutting"
    s_re.loc[s_re['manage_divi_x']==1,'manage_divi_x'] = 5
    #s_re.to_csv('Report/tes.csv')
    s1 = j.groupby("Group_code").sum(numeric_only=True)
    #s1.to_csv('Report/S.csv')
    lx =j.groupby("Group_code").sum(numeric_only=True)
    #lx.to_csv('Report/Lx.csv')
    l1= j.groupby("Group_code").min()
    #l1.to_csv('Report/L.csv')
    mer1 = l1.merge(s1,how='left',on='Group_code')
    #mer1.to_csv('Report/mer1.csv')
    re1 = mer1.loc[:, ["GROUP3", "GROUP4","DIAMETER1_x","DIAMETER2_x","WIDTH_x","THICKNESS_x","THICKNESS2_x","LENGTH_x","COLOR_NAME","CUSTOMERCODE","CURRENT_STOCK_y","MEMO_y","OVT","manage_divi_x","div","END_IN_DATE"]]
    s_re1 = re1.sort_values(by=['manage_divi_x','GROUP4','DIAMETER1_x','THICKNESS_x','LENGTH_x'])
    s_re1.loc[s_re1['manage_divi_x']==0,'div'] ="For Cutting"
    s_re1.loc[s_re1['manage_divi_x']==0,'manage_divi_x'] =3
    s_re1.loc[s_re1['manage_divi_x']==1,'div'] ="For Sales"
    s_re1.loc[s_re1['manage_divi_x']==1,'manage_divi_x'] =4
    final = pd.concat([coil_slit,s_re1,s_re])
    final.loc[final['THICKNESS2_x']!=0,'THICKNESS_x'] = final['THICKNESS2_x']
    final.loc[final['MEMO_y'].isna(),'MEMO_y'] = final['CURRENT_STOCK_y']
    ex_final=final.loc[:,["GROUP3", "GROUP4","DIAMETER1_x","DIAMETER2_x","WIDTH_x","THICKNESS_x","LENGTH_x","COLOR_NAME","CUSTOMERCODE","CURRENT_STOCK_y","MEMO_y","OVT","manage_divi_x","div","END_IN_DATE"]]
    de = ex_final.rename(columns={'CUSTOMERCODE':'CUSTOMER_CODE'})
    ex1_final = de.merge(cus,how='left',on='CUSTOMER_CODE')
    #ex_final = final.merge(cus,how='left',on='C')
    ex2_final = ex1_final.loc[:,["GROUP3", "GROUP4","DIAMETER1_x","DIAMETER2_x","WIDTH_x","THICKNESS_x","LENGTH_x","COLOR_NAME","E_NAME","CURRENT_STOCK_y","MEMO_y","OVT","manage_divi_x","div","END_IN_DATE"]]
    ex2_final.loc[ex2_final['LENGTH_x']==0,'CURRENT_STOCK_y'] = 0
    ex3_final = ex2_final.round({'MEMO_y':0})
    ex3_final['END_IN_DATE'] = pd.to_datetime(ex3_final['END_IN_DATE']).dt.date
    ex3_final.to_csv('Report/4_Stock_expired.csv')


def pipe_gr ():
    #pipe = pd.read_csv('Database/Good_sales.csv')
    bom = pd.read_csv('Database/BOM.csv')
    #bom_map = pd.read_csv('Database/BOM_map.csv')
    itemcode = pd.read_csv('Database/Itemcode.csv')
    #codename = pd.read_csv('Database/Code_Name.csv')
    

    bom.loc[bom['ParentThickness2'] == 0,'PIPE'] = bom['ParentItemCode'].astype(str)+bom['ParentLength'].astype(str)
    bom.loc[bom['ParentThickness2'] != 0,'SLIT'] = bom['ParentItemCode'].astype(str)+bom['ParentThickness2'].astype(str)
    bom.loc[(bom['ParentThickness2'] == 0) & (bom['ChildLength'] == 0),'SLIT'] = bom['ChildItemCode'].astype(str)+bom['ChildThickness2'].astype(str)
    bom.loc[bom['ParentThickness2'] != 0,'COIL'] = bom['ChildItemCode'].astype(str)+bom['ChildThickness2'].astype(str)
    bom.loc[(bom['ParentThickness2'] == 0) & (bom['ChildLength'] != 0),'PRO1'] = bom['ChildItemCode'].astype(str)+bom['ChildLength'].astype(str)
    lv1bom  = bom.rename(columns={'ParentItemCode':'ITEM_CODE'})
    v1 = lv1bom.filter(['PIPE','PRO1','ParentQty','ProcessCode','SLIT'])
    rename_v1 = v1.rename(columns={'PIPE':'PRO1','PRO1':'PRO2','ParentQty':'ParentQty_PRO2','ProcessCode':'ProcessCode_PRO2','SLIT':'SLIT_PRO2'})
    rename_v1_cleaned = rename_v1.dropna(subset=['PRO1']) #(subset=['Age'])
    gr1 = lv1bom.merge(rename_v1_cleaned,how='left',on='PRO1')
    #gr1.to_csv('Database/Ex1.csv')
    pipe1 = lv1bom.filter(['PIPE','ITEM_CODE','ParentLength'])
    pipe2 = lv1bom.filter(['PRO1','ChildItemCode','ChildLength'])
    pipe2_rename = pipe2.rename(columns={'PRO1':'PIPE','ChildItemCode':'ITEM_CODE','ChildLength':'ParentLength'})
    pipe = pd.concat([pipe1, pipe2_rename], ignore_index=True)
    pipex = pipe[pipe['PIPE'].notna()]
    final_pipe = pipex.drop_duplicates(subset=['PIPE'])
    ex_itemcode_final_pipe = final_pipe.merge(itemcode,how='left',on='ITEM_CODE')

    ex_pipe = ex_itemcode_final_pipe.filter(['PIPE','ITEM_CODE','ITEM_NAME_V','DIAMETER1','DIAMETER2','THICKNESS','ParentLength','isSquare','DIAMETER_Origin','INPUT_DATE','MODIFY_DATE'])
    #ex_pipe.to_csv('Database/Pipe_list.csv')
    # tách tên & ghép item code ống.
    # ghep chuoi gia cong cua Slit-COIL
    s_c_list = gr1[gr1['PIPE'].isna()]
    s_c_list1 = s_c_list.filter(['SLIT','COIL','Main_Material'])
    s_c1 = s_c_list1.rename(columns={'COIL':'SLIT1','SLIT':'COIL','Main_Material':'Main_1'})
    slit3 = s_c_list1.rename(columns={'SLIT':'SLIT1','COIL':'SLIT2','Main_Material':'Main_2'})
    slit4 = s_c_list1.rename(columns={'SLIT':'SLIT2','COIL':'SLIT3','Main_Material':'Main_3'})
    slit5 = s_c_list1.rename(columns={'SLIT':'SLIT3','COIL':'SLIT4','Main_Material':'Main_4'})
    slit6 = s_c_list1.rename(columns={'SLIT':'SLIT4','COIL':'SLIT5','Main_Material':'Main_5'})
    slit7 = s_c_list1.rename(columns={'SLIT':'SLIT5','COIL':'SLIT6','Main_Material':'Main_6'})

    gr_s_c = s_c_list1.merge(s_c1,how='left',on='COIL')
    gr_s_c2 = gr_s_c.merge(slit3,how='left',on='SLIT1')
    gr_s_c3 = gr_s_c2.merge(slit4,how='left',on='SLIT2')
    gr_s_c4 = gr_s_c3.merge(slit5,how='left',on='SLIT3')
    gr_s_c5 = gr_s_c4.merge(slit6,how='left',on='SLIT4')
    gr_s_c6 = gr_s_c5.merge(slit7,how='left',on='SLIT5')

    gr_s_c6.loc[gr_s_c6['SLIT6'].isna(),'MAIN_COIL'] = gr_s_c6['SLIT5']
    gr_s_c6.loc[gr_s_c6['SLIT5'].isna(),'MAIN_COIL'] = gr_s_c6['SLIT4']
    gr_s_c6.loc[gr_s_c6['SLIT4'].isna(),'MAIN_COIL'] = gr_s_c6['SLIT3']
    gr_s_c6.loc[gr_s_c6['SLIT3'].isna(),'MAIN_COIL'] = gr_s_c6['SLIT2']
    gr_s_c6.loc[gr_s_c6['SLIT2'].isna(),'MAIN_COIL'] = gr_s_c6['SLIT1']
    gr_s_c6.loc[gr_s_c6['SLIT1'].isna(),'MAIN_COIL'] = gr_s_c6['COIL']

    gr_s_c6.loc[gr_s_c6['SLIT6'].isna(),'MAIN'] = gr_s_c6['Main_5']
    gr_s_c6.loc[gr_s_c6['SLIT5'].isna(),'MAIN'] = gr_s_c6['Main_4']
    gr_s_c6.loc[gr_s_c6['SLIT4'].isna(),'MAIN'] = gr_s_c6['Main_3']
    gr_s_c6.loc[gr_s_c6['SLIT3'].isna(),'MAIN'] = gr_s_c6['Main_2']
    gr_s_c6.loc[gr_s_c6['SLIT2'].isna(),'MAIN'] = gr_s_c6['Main_1']
    gr_s_c6.loc[gr_s_c6['SLIT1'].isna(),'MAIN'] = gr_s_c6['Main_Material']
    slit_coil_router = gr_s_c6.filter(['SLIT','MAIN_COIL','MAIN','SLIT1','SLIT2','SLIT3','SLIT4','SLIT5','SLIT6'])
    #slit_coil_router.to_csv('Database/Slit_Coil_router.csv') # vẽ lại chu trình phân sợi

    # Định nghĩa chuỗi slit-coil
    s_coil = lv1bom.filter(['SLIT','ITEM_CODE','ParentThickness2'])
    s_coil_x = s_coil[s_coil['ParentThickness2']!=0]
    s_coil_x_drop_dup = s_coil_x.drop_duplicates(subset=['SLIT'])
    s_coil_itemcode = s_coil_x_drop_dup.merge(itemcode,how='left',on='ITEM_CODE')
    s_coil_itemcode_filter = s_coil_itemcode.filter(['SLIT','ITEM_CODE','ITEM_NAME_V','ParentThickness2','WIDTH','INV_DV','INPUT_DATE','MODIFY_DATE'])
    #s_coil_itemcode_filter.to_csv('Database/slit.csv')
    # tách tên & ghép item slit coil.
    m_coil = lv1bom.filter(['COIL','ChildItemCode','ChildThickness2','Main_Material','INPUT_DATE'])
    #m_coil.to_csv('Database/coil.csv')
    m_coil_x = m_coil[m_coil['ChildThickness2']!=0]
    m_coil_drop_dup = m_coil_x.drop_duplicates(subset='COIL')
    rename_m_coil = m_coil_drop_dup.rename(columns={'ChildItemCode':'ITEM_CODE'})
    m_coil_itemcode = rename_m_coil.merge(itemcode,how='left',on='ITEM_CODE')
    m_coil_itemcode_filter = m_coil_itemcode.filter(['COIL','ITEM_CODE','ITEM_NAME_V','ChildThickness2','WIDTH','Main_Material','INV_DV','INPUT_DATE','MODIFY_DATE'])
    #m_coil_itemcode_filter.to_csv('Database/coil.csv')
    fix_1 = m_coil_itemcode_filter.rename(columns={'COIL':'MAIN_COIL','ChildThickness2':'Thickness_Coil','WIDTH':'WIDTH_COIL'})
    fix_2 = s_coil_itemcode_filter.rename(columns={'SLIT':'COIL_SLIT','ParentThickness2':'Thickness_Slit','WIDTH':'WIDTH_SLIT'})
    #SLIT_COIL = pd.concat([fix_1, fix_2], ignore_index=True)
    #SLIT_COIL_filter = SLIT_COIL.filter(['COIL_SLIT','ITEM_CODE','ITEM_NAME_V','Thickness_Slit','Thickness_Coil','WIDTH','INV_DV','Main_Material','MODIFY_DATE','INPUT_DATE'])
    
    rule_slit = slit_coil_router.merge(s_coil_itemcode_filter,how='left',on='SLIT')
    rule_slit1 = rule_slit.merge(fix_1,how='left',on='MAIN_COIL')
    rule_slitx = rule_slit1.filter(['SLIT','ITEM_CODE_x','ITEM_NAME_V_x','ParentThickness2','WIDTH','INV_DV_x','MAIN_COIL','ITEM_CODE_y','ITEM_NAME_V_y','Thickness_Coil','WIDTH_COIL','MAIN'])
    rule_slitx_final = rule_slitx[rule_slitx['MAIN']==1]
    #rule_slitx_final.to_csv('Database/Slit_coil.csv')


    #tách tên & ghép item Coil.
    # Gia cong lan 03
    v2 = lv1bom.filter(['PIPE','SLIT','PRO1','ParentQty','ProcessCode'])
    rename_v2 = v2.rename(columns={'PIPE':'PRO2','PRO1':'PRO3','ParentQty':'ParentQty_PRO3','ProcessCode':'ProcessCode_PRO3','SLIT':'SLIT_PRO3'})
    rename_v2_cleaned = rename_v2.dropna(subset=['PRO2'])
    gr2 = gr1.merge(rename_v2_cleaned,how='left',on='PRO2')
    # Gia cong lan 04
    v3 = lv1bom.filter(['PIPE','SLIT','PRO1','ParentQty','ProcessCode'])
    rename_v3 = v3.rename(columns={'PIPE':'PRO3','PRO1':'PRO4','ParentQty':'ParentQty_PRO4','ProcessCode':'ProcessCode_PRO4','SLIT':'SLIT_PRO4'})
    rename_v3_cleaned = rename_v3.dropna(subset=['PRO3'])
    gr3 = gr2.merge(rename_v3_cleaned,how='left',on='PRO3')
    # Gia cong lan 05
    v4 = lv1bom.filter(['PIPE','SLIT','PRO1','ParentQty','ProcessCode'])
    rename_v4 = v4.rename(columns={'PIPE':'PRO4','PRO1':'PRO5','ParentQty':'ParentQty_PRO5','ProcessCode':'ProcessCode_PRO5','SLIT':'SLIT_PRO5' })
    rename_v4_cleaned = rename_v4.dropna(subset=['PRO4'])
    gr4 = gr3.merge(rename_v4_cleaned,how='left',on='PRO4')
    # Gia cong lan 06
    v5 = lv1bom.filter(['PIPE','SLIT','PRO1','ParentQty','ProcessCode'])
    rename_v5 = v5.rename(columns={'PIPE':'PRO5','PRO1':'PRO6','ParentQty':'ParentQty_PRO6','ProcessCode':'ProcessCode_PRO6','SLIT':'SLIT_PRO6' })
    rename_v5_cleaned = rename_v5.dropna(subset=['PRO5'])
    gr5 = gr4.merge(rename_v5_cleaned,how='left',on='PRO5')

    gr5.loc[gr5['SLIT'].isna(),'SLIT'] = gr5['SLIT_PRO2']
    gr5.loc[gr5['SLIT'].isna(),'SLIT'] = gr5['SLIT_PRO3']
    gr5.loc[gr5['SLIT'].isna(),'SLIT'] = gr5['SLIT_PRO4']
    gr5.loc[gr5['SLIT'].isna(),'SLIT'] = gr5['SLIT_PRO5']
    gr5.loc[gr5['SLIT'].isna(),'SLIT'] = gr5['SLIT_PRO6']

    gr5.loc[(gr5['ParentQty'] == 1) & (gr5['ProcessCode'] == 'CO'),'PRO'] = gr5['PIPE']
    gr5.loc[(gr5['ParentQty_PRO2'] == 1) & (gr5['ProcessCode_PRO2'] == 'CO'),'PRO'] = gr5['PRO1']
    gr5.loc[(gr5['ParentQty_PRO3'] == 1) & (gr5['ProcessCode_PRO3'] == 'CO'),'PRO'] = gr5['PRO2']
    gr5.loc[(gr5['ParentQty_PRO4'] == 1) & (gr5['ProcessCode_PRO4'] == 'CO'),'PRO'] = gr5['PRO3']
    gr5.loc[(gr5['ParentQty_PRO5'] == 1) & (gr5['ProcessCode_PRO5'] == 'CO'),'PRO'] = gr5['PRO4']
    gr5.loc[(gr5['ParentQty_PRO6'] == 1) & (gr5['ProcessCode_PRO6'] == 'CO'),'PRO'] = gr5['PRO5']
    #gr5.to_csv('Database/BOM_FIX.csv')
    gr5.loc[gr5['PRO'].isna(),'PRO'] = gr5['PRO6']
    gr5.loc[gr5['PRO'].isna(),'PRO'] = gr5['PRO5']
    gr5.loc[gr5['PRO'].isna(),'PRO'] = gr5['PRO4']
    gr5.loc[gr5['PRO'].isna(),'PRO'] = gr5['PRO3']
    gr5.loc[gr5['PRO'].isna(),'PRO'] = gr5['PRO2']
    gr5.loc[gr5['PRO'].isna(),'PRO'] = gr5['PRO1']
    gr5.loc[gr5['ParentQty']>1,'RATE'] = gr5['ParentQty']
    gr5.loc[gr5['ParentQty_PRO2']>1,'RATE'] = gr5['ParentQty_PRO2']
    gr5.loc[gr5['ParentQty_PRO3']>1,'RATE'] = gr5['ParentQty_PRO3']
    gr5.loc[gr5['ParentQty_PRO4']>1,'RATE'] = gr5['ParentQty_PRO4']
    gr5.loc[gr5['ParentQty_PRO5']>1,'RATE'] = gr5['ParentQty_PRO5']
    gr5.loc[gr5['ParentQty_PRO6']>1,'RATE'] = gr5['ParentQty_PRO6']
    

    #gr5_extra = gr5.filter(['PIPE','RATE','PRO','PRO1','PRO2','PRO3','PRO4','PRO5','PRO6','SLIT','COIL','Main_Material','INPUT_DATE'])
    #gr5_extra.to_csv('Database/Pipe_router.csv')
    gr5_filter = gr5.filter(['PIPE','ITEM_CODE','ParentLength','RATE','PRO','SLIT','ParentThickness2','COIL','INPUT_DATE'])

    #lọc itemcode
    itemcode_pipe = itemcode.filter(['ITEM_CODE','ITEM_NAME_V','DIAMETER1','DIAMETER2','THICKNESS','WIDTH'])
    gr6 = gr5_filter.merge(itemcode_pipe,how='left',on='ITEM_CODE')
    gr6_filter = gr6.filter(['PIPE','ITEM_CODE','ITEM_NAME_V','DIAMETER1','DIAMETER2','THICKNESS','ParentLength','RATE','PRO', 'SLIT','ParentThickness2','WIDTH','COIL','INPUT_DATE'])
    gr6_filter.loc[(gr6_filter['PIPE'].isna())&(gr6_filter['ParentLength']==0),'ITEM_NAME_SLIT'] = gr6_filter['ITEM_NAME_V']
    gr6_filter.loc[(gr6_filter['PIPE'].isna())&(gr6_filter['ParentLength']==0),'ITEM_CODE_SLIT'] = gr6_filter['ITEM_CODE']
    
    ex1_pipe = ex_pipe.rename(columns={'PIPE':'PRO','ITEM_CODE':'ITEM_CODE_PRO','ITEM_NAME_V':'ITEM_NAME_PRO','DIAMETER1':'DIAMETER1_PRO','DIAMETER2':'DIAMETER2_PRO','THICKNESS':'THICKNESS_PRO','ParentLength':'LENGTH_PRO'})

    gr7 = gr6_filter.merge(ex1_pipe,how='left',on='PRO')
    gr7_filter = gr7.filter(['PIPE','ITEM_CODE','ITEM_NAME_V','DIAMETER1','DIAMETER2',	'THICKNESS','ParentLength',	'RATE',	'PRO','ITEM_CODE_PRO',	'ITEM_NAME_PRO',	'DIAMETER1_PRO',	'DIAMETER2_PRO','THICKNESS_PRO','LENGTH_PRO','SLIT'])
    gr7_final = gr7_filter[gr7_filter['PIPE'].notna()]
    gr7_final.loc[gr7_final['RATE'].isna(),'RATE'] = 1
    gr8 = gr7_final.merge(rule_slitx_final,how='left',on='SLIT')

    new = gr8['ITEM_NAME_V'].str.split(" ",n=4, expand = True)
    gr8['Sur'] = new[0]
    gr8['Type'] = new[1]
    gr8['PIPE_Name'] = new[3]
    new2 = gr8['ITEM_NAME_PRO'].str.split(" ",n=4, expand = True)
    gr8['Sur_pro'] = new2[0]
    gr8['Type_pro'] = new2[1]
    gr8['PRO_name'] = new2[3]
    new3 = gr8['ITEM_NAME_V_x'].str.split(" ",n=4, expand = True)
    gr8['Sur_slit'] = new3[0]
    gr8['Type_slit'] = new3[1]
    gr8['Slit_name'] = new3[4]
    new4 = gr8['ITEM_NAME_V_y'].str.split(" ",n=4, expand = True)
    gr8['Sur_coil'] = new4[0]
    gr8['Type_coil'] = new4[1]
    gr8['Coil_name'] = new4[3]

    gr9 = gr8.filter(['PIPE','ITEM_CODE','Sur','Type','PIPE_Name','DIAMETER1'	,'DIAMETER2','THICKNESS','ParentLength','RATE','PRO','ITEM_CODE_PRO','Sur_pro',	'Type_pro',	'PRO_name','DIAMETER1_PRO','DIAMETER2_PRO',	'THICKNESS_PRO','LENGTH_PRO','SLIT','ITEM_CODE_x','Sur_slit','Type_slit','Slit_name','ParentThickness2','WIDTH','INV_DV_x',	'MAIN_COIL','ITEM_CODE_y','Sur_coil','Type_coil','Coil_name','Thickness_Coil','WIDTH_COIL'])
    gr9_drop = gr9.drop_duplicates(subset=['PIPE'])
    gr9_drop_sort = gr9_drop.sort_values(by=['Sur','PIPE_Name','DIAMETER1','DIAMETER2','THICKNESS','ParentLength'])
    gr9_drop_sort.to_csv('Database/BOM_map2.csv')

def Stock_Group():
    Stock_ima = pd.read_csv('Database/Stock_ima.csv')
    BOM = pd.read_csv('Database/BOM_map2.csv')
    Cus = pd.read_csv('Database/Cus.csv')
    Stock_ima.loc[Stock_ima['GROUP1']==2,'Group_code'] =  Stock_ima['ITEM_STATUS'].astype(str) + Stock_ima['COLOR_NAME'].astype(str)
    sum_stock = Stock_ima.groupby('ITEM_STATUS').sum(numeric_only=True)
    name = Stock_ima.groupby('ITEM_STATUS').first()
    Group1 = sum_stock.merge(name,how='left',on='ITEM_STATUS')
    Group1_filter = Group1.filter(['MEMO_ERR_STATUS','CUSTOMERCODE','GROUP1_y','GROUP3','GROUP4','GROUP5','DIAMETER1_y','DIAMETER2_y','THICKNESS_y','THICKNESS2_y','WIDTH_y','LENGTH_y','COLOR_NAME','PROCESSING_NAME','CURRENT_STOCK_x','UNIT_ITEM','MEMO_x'])
    Pipe = Group1_filter.loc[Group1_filter['GROUP1_y']==2]
    Slit_coil = Group1_filter.loc[Group1_filter['GROUP1_y']==1]
    Pipe_rename = Pipe.rename(columns={'MEMO_ERR_STATUS':'PIPE','MEMO_x':'WEIGHT'})
    Mapping_pipe = Pipe_rename.merge(BOM,how='left',on='PIPE')
    Mapping_pipe_filter = Mapping_pipe.filter(['CUSTOMERCODE','GROUP3',	'GROUP4','GROUP5','DIAMETER1_y',	'DIAMETER2_y',	'THICKNESS_y',	'LENGTH_y','COLOR_NAME','PROCESSING_NAME','CURRENT_STOCK_x','UNIT_ITEM','WEIGHT','INV_DV_x','Sur_pro','PRO_name','DIAMETER1_PRO',	'DIAMETER2_PRO','THICKNESS_PRO','LENGTH_PRO','RATE','PRO','PRO_name'])
    Mapping_pipe_filter.loc[Mapping_pipe_filter['PRO_name'].notna(),'ITEM_STATUS'] = Mapping_pipe_filter['CUSTOMERCODE'].astype(str)+Mapping_pipe_filter['PRO'].astype(str)+Mapping_pipe_filter['COLOR_NAME'].astype(str)
    #lv2_pipe = Pipe.rename(columns={'ITEM_STATUS':'PRO_I'})
    Mapping2_pipe = Mapping_pipe_filter.merge(Pipe,how='left',on='ITEM_STATUS')
    Mapping2_pipe_filter = Mapping2_pipe.filter(['CUSTOMERCODE_x','GROUP3_x','GROUP4_x','DIAMETER1_y_x','DIAMETER2_y_x','THICKNESS_y_x','LENGTH_y_x','COLOR_NAME_x','PROCESSING_NAME_x','CURRENT_STOCK_x_x','UNIT_ITEM_x','WEIGHT','INV_DV_x','PRO_name','GROUP5_x','DIAMETER1_PRO','DIAMETER2_PRO','THICKNESS_PRO','LENGTH_PRO','COLOR_NAME_y','PROCESSING_NAME_y',	'CURRENT_STOCK_x_y'	,'UNIT_ITEM_y','RATE'])
    Mapping2_pipe_filter_sort = Mapping2_pipe_filter.sort_values(by=['CUSTOMERCODE_x', 'GROUP3_x','GROUP4_x','DIAMETER1_y_x','DIAMETER2_y_x','THICKNESS_y_x','LENGTH_y_x','COLOR_NAME_x','PROCESSING_NAME_x'],ascending=True)
    Cus_Mapping2_pipe_filter_sort = Mapping2_pipe_filter_sort.rename(columns={'CUSTOMERCODE_x':'CUSTOMER_CODE'})
    final = Cus_Mapping2_pipe_filter_sort.merge(Cus,how='left',on='CUSTOMER_CODE')
    finalx = final.filter(['E_NAME','GROUP3_x','GROUP4_x','DIAMETER1_y_x','DIAMETER2_y_x','THICKNESS_y_x','LENGTH_y_x','COLOR_NAME_x','PROCESSING_NAME_x','CURRENT_STOCK_x_x','UNIT_ITEM_x','WEIGHT','INV_DV_x','PRO_name','GROUP5_x','DIAMETER1_PRO','DIAMETER2_PRO','THICKNESS_PRO','LENGTH_PRO','COLOR_NAME_y','PROCESSING_NAME_y',	'CURRENT_STOCK_x_y'	,'UNIT_ITEM_y','RATE'])
    finalx_y = finalx.sort_values(by=['E_NAME','GROUP3_x','GROUP4_x','DIAMETER1_y_x','DIAMETER2_y_x','THICKNESS_y_x','LENGTH_y_x'],ascending=True)
    #finalx_y_name = finalx_y.rename(columns={'E_NAME':'CUSTOMER','GROUP3_x':'SURFACE','DIAMETER1_y_x':'DIAMETER1','DIAMETER2_y_x':'DIAMETER2','THICKNESS_y_x':'THICKNESS','LENGTH_y_x':'LENGTH','COLOR_NAME_x':'Beat','PROCESSING_NAME_x':'PROCESS','CURRENT_STOCK_x_x':'Q.TY','UNIT_ITEM_x':'UNIT','PRO_name':'MATERIAL_PIPE','GROUP5_x':'MATERIAL_COIL','COLOUR_NAME_y':'BEAT_PRO'})
    finalx_y.loc[finalx_y['WEIGHT'].notna(),'INV_DV_x'] = "KG"
    finalx_y.loc[finalx_y['PRO_name'].isna(),'PRO_name'] = finalx_y['GROUP4_x']
    finalx_y.loc[finalx_y['DIAMETER1_PRO'].isna(),'DIAMETER1_PRO'] = finalx_y['DIAMETER1_y_x']
    finalx_y.loc[finalx_y['DIAMETER2_PRO'].isna(),'DIAMETER2_PRO'] = finalx_y['DIAMETER2_y_x']
    finalx_y.loc[finalx_y['THICKNESS_PRO'].isna(),'THICKNESS_PRO'] = finalx_y['THICKNESS_y_x']
    finalx_y.loc[finalx_y['LENGTH_PRO'].isna(),'LENGTH_PRO'] = finalx_y['LENGTH_y_x']
    finalx_y_z = finalx_y.round({'WEIGHT':0})
    finalx_y_z.replace({None: ' '},inplace=True)
    finalx_y_z.replace(np.nan, ' ',regex=True,inplace=True)
    finalx_y_z.to_excel('Report/3_Pipe_Stocklist.xlsx')
    #Mapping2_pipe_filter.to_excel('Report/Pipe_test.xlsx')


def main():
    get_stock_fur()
    get_stock_ima()
    get_BOM()
    pipe_gr()
    #BOM_Compare()
    #data_analyst()
    #stock_analyst()
    stock_expried()
    pipe_expried()
    Stock_Group()
    get_po()

if __name__ == "__main__":
    main()




