# -*- coding: utf-8 -*-
"""
Created on Mon Jun  7 14:15:39 2021

@author: mjmel
"""

import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
from datetime import date
from tqdm import tqdm
import math


#############################################################################
##################         9018-28-29-9330         ##########################
#############################################################################

notif_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9018-28-29-9330\Notifications.xlsx'
IB_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9018-28-29-9330\IB.xlsx'
contracts_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9018-28-29-9330\Contract Base.xlsx'
sap_mvt_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9018-28-29-9330\SAP MVT.xlsx'



#############################################################################
##################               9450               #########################
#############################################################################

notif_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9450\Notifications.xlsx'
IB_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9450\Historic IB.xlsx'
contracts_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9450\Contract Base.xlsx'
sap_mvt_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9450\Part Movement.xlsx'

#############################################################################
##################               9450C              #########################
#############################################################################

notif_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9450C\9450C Ec Sc Notification 2015 To 2021 Extraction.xlsx'
IB_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9450C\9450C Ec Sc IB February 2021 Extraction.xlsx'
contracts_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9450C\Contract created between 01-01-200 and 27-07-2021- From Renewal Query in BW.xlsx'
sap_mvt_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9450C\SAP Parts Mvt.xlsx'

#############################################################################
##################               9410               #########################
#############################################################################
 
notif_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9410\Notifications.xlsx'
IB_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9410\Historic IB.xlsx'
contracts_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9410\Contract Base.xlsx'
sap_mvt_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9410\Part Movement.xlsx'



notif = pd.read_excel(notif_path)
notif['Notif. ID'] = notif['Notif. ID'].astype(int).astype(str)
notif['CS Order ID'] = notif['CS Order ID'].astype(str)

IB = pd.read_excel(IB_path, header=2)
IB['Ship-to Classification'] = IB['Ship-to Classification'].astype(str).str.replace('.0', '').replace('','10')
contracts = pd.read_excel(contracts_path, header = 3, sheet_name = 'ZANALYSIS_PATTERN')



sap_mvt = pd.read_excel(sap_mvt_path)
sap_mvt['Material']=sap_mvt['Material'].astype('str')
sap_mvt = sap_mvt[~sap_mvt['Material'].str.contains("REN")]
sap_mvt = sap_mvt.groupby(['Order','Material']).nth(0).drop(columns='Quantity').merge(sap_mvt.groupby(['Order','Material'])['Quantity'].sum(),how='left',on=['Order','Material'])
sap_mvt.reset_index(inplace=True)
neg_qty = [x for x in sap_mvt['Quantity'].unique() if x<0]
sap_mvt=sap_mvt[~sap_mvt['Quantity'].isin(neg_qty)]

sap_mvt['Order']=sap_mvt['Order'].astype('str')

####Add quantity in notif
notif['Quantity']=notif['CS Order ID'].apply(lambda x: np.sum(sap_mvt[sap_mvt['Order']==x]['Quantity'].values) if x in sap_mvt['Order'].unique() else np.NaN)
notif['Parts changed']=notif['CS Order ID'].apply(lambda x: {sap_mvt[sap_mvt['Order']==x]['Material'].values[i]:sap_mvt[sap_mvt['Order']==x]['Quantity'].values[i] for i in range(len(sap_mvt[sap_mvt['Order']==x]))} if x in sap_mvt['Order'].unique() else np.NaN)
neg_qty = [x for x in notif['Quantity'].unique() if x<0]
notif=notif[~notif['Quantity'].isin(neg_qty)]

notif['Number of parts changed'] = notif['CS Order ID'].apply(lambda x: len(sap_mvt[(sap_mvt['Order']==x)&(sap_mvt['Quantity']!=0)]['Material'].unique()) if x in sap_mvt['Order'].unique() else np.NaN)


#####Start and end scope DDSO for each printer
print('-------------------------------------')
print('DDSO scope calculation started')

days_threshold = 35
printers = list(set(IB['Equipment'].to_list()))
DDSO_scope = pd.DataFrame(columns=['Equip. Serial Nb', 'Equip. Starting date', 'Start warranty',
       'End warranty', 'Start contract',
       'End contract', 'Start SCOPE', 'End SCOPE'])
error_printers = []
for i, index in enumerate(tqdm(printers)):  
    DDSO_scope.at[index, 'Equip. Serial Nb'] = index
    DDSO_scope.at[index, 'Equip. Starting date'] = datetime.utcfromtimestamp((IB[IB['Equipment']==index]['Equip. Starting date'].values[0]).tolist()/1e9).date()
    DDSO_scope.at[index, 'Start warranty'] = datetime.utcfromtimestamp((IB[IB['Equipment']==index]['Equip. Starting date'].values[0]).tolist()/1e9).date()
    try:
        try:
            DDSO_scope.at[index, 'End warranty'] = IB[IB['Equipment']==index][IB.columns[22]].values[0].date()
        except:
            if pd.isnull((pd.to_datetime(IB[IB['Equipment']==index][IB.columns[22]].values[0]).date())):
                DDSO_scope.at[index, 'End warranty'] = DDSO_scope.at[index, 'Start warranty'] + relativedelta(years=1)
            else:
                DDSO_scope.at[index, 'End warranty'] = pd.to_datetime(IB[IB['Equipment']==index][IB.columns[22]].values[0]).date()
    except:
        DDSO_scope.at[index, 'End warranty'] = DDSO_scope.at[index, 'Start warranty'] + relativedelta(years=1)
    if DDSO_scope.at[index, 'Start warranty']> DDSO_scope.at[index, 'End warranty']:
        DDSO_scope.at[index, 'Start warranty'] = DDSO_scope.at[index, 'End warranty'] - relativedelta(years=1)
        
    valid_from = []
    valid_to = []
    if len(contracts[contracts['Equipment']==index])>0:
        printer_contracts = contracts[contracts['Equipment']==index][['Contract Valid From', 'Contract Valid To', 'Contract Nature']].sort_values(by=['Contract Valid From'])
        valid_from = printer_contracts['Contract Valid From'].to_list()
        valid_to = printer_contracts['Contract Valid To'].to_list()
        dates_contracts = sum([[valid_from[i], valid_to[i]] for i in range(len(valid_from))],[])
        start_date_contract = dates_contracts[0].date()
        if len(dates_contracts)==2:
            end_date_contract = dates_contracts[-1].date()
        else:
            break_status = False
            for k in range(2, len(dates_contracts), 2):
                if (dates_contracts[k] - dates_contracts[k-1]).days < days_threshold:
                    pass
                else:
                    end_date_contract = dates_contracts[k-1].date()
                    break_status = True
                    break
            if not break_status:
                end_date_contract = dates_contracts[-1].date()
        DDSO_scope.at[index, 'Start contract'] = start_date_contract
        DDSO_scope.at[index, 'End contract'] = end_date_contract
        DDSO_scope.at[index, 'Contract Nature'] = printer_contracts['Contract Nature'].values[-1]
    if len(contracts[contracts['Equipment']==index])>0:    
        dates_contracts_warranty = [DDSO_scope.at[index, 'Start warranty'],DDSO_scope.at[index, 'End warranty'],start_date_contract, end_date_contract]
    else:
        dates_contracts_warranty = [DDSO_scope.at[index, 'Start warranty'],DDSO_scope.at[index, 'End warranty']]        
    start_date_contract_warranty = dates_contracts_warranty[0]
    if len(dates_contracts_warranty)>2:
        if (dates_contracts_warranty[2] - dates_contracts_warranty[1]).days < days_threshold:
            end_date_contract_warranty = max(dates_contracts_warranty[1],dates_contracts_warranty[3])
        else:
            end_date_contract_warranty =dates_contracts_warranty[1]
    else:
        end_date_contract_warranty = dates_contracts_warranty[-1]
    DDSO_scope.at[index, 'Start SCOPE'] = start_date_contract_warranty
    DDSO_scope.at[index, 'End SCOPE'] = end_date_contract_warranty


printers_under_scope = DDSO_scope[DDSO_scope['End SCOPE']>=date(2021, 6, 1)]['Equip. Serial Nb'].unique()
print('-------------------------------------')
print('DDSO scope calculation finished')



####Filters 
#notif['Maint. Activity Type'].unique()
sub_codes = ['010', '100', '120', '130', '140', '145', '150', '155', '160', '165',
             '170', '175', '180', '280', '281', '282']
sales_channel = ['MI_Direct']
#IB['Ind. family - lev. 1'].unique()
sectors_filter = ['OTHER INDUSTRIALS', 'GRAPHICS', 'PATIENT CARE',
       'NON FOOD CONSUMER PR', 'AUTOMOTIVE, VEHICLES', 'FOODS',
       'PHARMACEUTICALS & ME', 'FOREST PRODUCTS', 'BEVERAGES',
       'COSMETICS & TOILETRI', 'OTHER CONSUMER DURAB',
       'CABLES, TUBES & PROF', 'ELECTRICAL & ELECTRO', 'LOGISTICS',
       'PLASTIC & RUBBER COM']
#IB['Sales Organization'].unique()
sales_org = ['AT01', 'BE00', 'CA01', 'CH01', 'DE01', 'DK01', 'ES01', 'FI01',
       'IT01', 'NL01', 'PT01', 'SE01', 'US01', 'BRA1', 'CNA1', 'JP01',
       'RU01', 'SGA1', 'SGA3', 'FRA1', 'GBA1', 'FRA3', 'FRA4', 'INA1',
       'TWA1', 'ARA1', 'AUA1', 'MXA1', 'MYA1', 'THA1', 'US03', 'KRA1']
#IB['Ship-to Classification'].unique()
ship_to_classification = ['10', '11', '15', '16', '51', '52', '53', '98']



####Filter IB and notif 
####IB
print('-------------------------------------')
print('Filtering IB')

IB_filtered = IB[(IB['Sales Channel'].isin(sales_channel)) &\
                 IB['Ind. family - lev. 1'].isin(sectors_filter) &\
                     IB['Sales Organization'].isin(sales_org ) &\
                         IB['Ship-to Classification'].isin(ship_to_classification)]
 
    
printers_IB_filtered = set(IB_filtered['Equipment'].to_list())
IB_filtered['Start SCOPE'] = IB_filtered['Equipment'].apply(lambda x: DDSO_scope[DDSO_scope['Equip. Serial Nb']==x]['Start SCOPE'].values[0])
IB_filtered['End SCOPE'] = IB_filtered['Equipment'].apply(lambda x: DDSO_scope[DDSO_scope['Equip. Serial Nb']==x]['End SCOPE'].values[0])
IB_filtered['Contract Nature'] = IB_filtered['Equipment'].apply(lambda x: DDSO_scope[DDSO_scope['Equip. Serial Nb']==x]['Contract Nature'].values[0])
extraction_date = date(2021, 3, 31)     
IB_filtered['Status'] = IB_filtered.apply(lambda x: 1 if (extraction_date <= x['End SCOPE']) and (extraction_date >= x['Start SCOPE']) else 0, axis=1)
IB_filtered_and_under_scope = IB_filtered[IB_filtered['Status']==1]


###Notif
print('-------------------------------------')
print('Filtering notif')

notif_filtered = notif[(notif['Maint. Activity Type'].isin(sub_codes))&\
                       (notif['Sales Channel'].isin(sales_channel))&\
                           (notif['Sup.Equip. Serial Nb'].isin(printers_IB_filtered))]
    

 
# len([x for x in set(notif['Equip. Serial Nb'].to_list()) if x in    printers])
# len([x for x in set(notif['Sup.Equip. Serial Nb'].to_list())   if x in    printers])




print('-------------------------------------')
print('Adding scope to notif')
  
notif_filtered['Start SCOPE'] = notif_filtered['Sup.Equip. Serial Nb'].apply(lambda x: DDSO_scope[DDSO_scope['Equip. Serial Nb']==x]['Start SCOPE'].values[0])
notif_filtered['End SCOPE'] = notif_filtered['Sup.Equip. Serial Nb'].apply(lambda x: DDSO_scope[DDSO_scope['Equip. Serial Nb']==x]['End SCOPE'].values[0])
notif_filtered['Contract Nature'] = notif_filtered['Sup.Equip. Serial Nb'].apply(lambda x: DDSO_scope[DDSO_scope['Equip. Serial Nb']==x]['Contract Nature'].values[0])

notif_filtered['Notif status'] = notif_filtered.apply(lambda x: 1 if (x['Notif. Date']<=x['End SCOPE']) and (x['Notif. Date']>=x['Start SCOPE']) else 0, axis=1)

notif_filtered_and_under_scope = notif_filtered[notif_filtered['Notif status']==1]

print('-------------------------------------')
print('Scope added to notif')

#####Calculate metrics
metrics = pd.DataFrame(columns = ['Metric', 'Value wo filters',
                                  'Value with filters', 'Value with filters and under scope DDSO'])


notif = notif.drop_duplicates(subset=['Notif. ID'])
notif_filtered = notif_filtered.drop_duplicates(subset=['Notif. ID'])
notif_filtered_and_under_scope = notif_filtered_and_under_scope.drop_duplicates(subset=['Notif. ID'])

############################# High level ##################################
####Add number of notifications 
notif_qty = len(notif)
notif_qty_filters = len(notif_filtered)
notif_qty_filters_under_scope = len(notif_filtered_and_under_scope)
serie = ['Qty of notifications', notif_qty, notif_qty_filters, notif_qty_filters_under_scope]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)

###Add number of cs orders
cs_orders_qty = len(notif['CS Order ID'].unique())
cs_orders_qty_filters = len(notif_filtered['CS Order ID'].unique())
cs_orders_qty_filters_under_scope = len(notif_filtered_and_under_scope['CS Order ID'].unique())
serie = ['Qty of CS Order', cs_orders_qty, cs_orders_qty_filters, cs_orders_qty_filters_under_scope]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)

###Add number of cs orders with part changement
cs_orders_qty_w_parts_changed = len(notif[notif['Quantity']>0]['CS Order ID'].unique())
cs_orders_qty_w_parts_changed_filters = len(notif_filtered[notif_filtered['Quantity']>0]['CS Order ID'].unique())
cs_orders_qty_w_parts_changed_filters_under_scope = len(notif_filtered_and_under_scope[notif_filtered_and_under_scope['Quantity']>0]['CS Order ID'].unique())
serie = ['Qty of CS Order with parts changed', cs_orders_qty_w_parts_changed,
         cs_orders_qty_w_parts_changed_filters, cs_orders_qty_w_parts_changed_filters_under_scope]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)

#######Qty notif without mention in sap and with activity part replaced#######
qty_notif_wo_sap_mvt_w_replaced_part = len(notif[(~notif['Parts changed'].notnull()) & (notif['Activity']=='Replaced part')])
qty_notif_wo_sap_mvt_w_replaced_part_filters = len(notif_filtered[(~notif_filtered['Parts changed'].notnull()) & (notif_filtered['Activity']=='Replaced part')])
qty_notif_wo_sap_mvt_w_replaced_part_filters_under_scope = len(notif_filtered_and_under_scope[(~notif_filtered_and_under_scope['Parts changed'].notnull()) & (notif_filtered_and_under_scope['Activity']=='Replaced part')])
serie = ['Qty of Notifications without SAP mvt and with Activity = Replaced part',
         qty_notif_wo_sap_mvt_w_replaced_part, qty_notif_wo_sap_mvt_w_replaced_part_filters,
         qty_notif_wo_sap_mvt_w_replaced_part_filters_under_scope]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)



#######Qty notif mention in sap with quantity equal to 0 and with activity part replaced#######
qty_notif_w_sap_mvt_qty_0_w_replaced_part = len(notif[(notif['Parts changed'].notnull()) &(notif['Quantity']==0) & (notif['Activity']=='Replaced part')])
qty_notif_w_sap_mvt_qty_0_w_replaced_part_filters = len(notif_filtered[(notif_filtered['Parts changed'].notnull()) &(notif['Quantity']==0) & (notif_filtered['Activity']=='Replaced part')])
qty_notif_w_sap_mvt_qty_0_w_replaced_part_filters_under_scope = len(notif_filtered_and_under_scope[(notif_filtered_and_under_scope['Parts changed'].notnull()) &(notif['Quantity']==0) & (notif_filtered_and_under_scope['Activity']=='Replaced part')])
serie = ['Qty of Notifications with SAP mvt but quantity equal to 0 and with Activity = Replaced part',
         qty_notif_w_sap_mvt_qty_0_w_replaced_part, 
         qty_notif_w_sap_mvt_qty_0_w_replaced_part_filters,
         qty_notif_w_sap_mvt_qty_0_w_replaced_part_filters_under_scope]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)



####Add quantity of printers
qty_printers = len(set(IB['Equipment'].to_list()))
qty_printers_filtered = len(set(IB_filtered['Equipment'].to_list()))   
qty_printers_filtered_under_scope = len(set(IB_filtered_and_under_scope['Equipment'].to_list()))                      
serie = ['Qty of Printers in Global IB', qty_printers, qty_printers_filtered, qty_printers_filtered_under_scope]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)

####Add quantity of printers in notif
qty_printers_notif = len(set(notif['Sup.Equip. Serial Nb'].to_list()))
qty_printers_notif_filtered = len(set(notif_filtered['Sup.Equip. Serial Nb'].to_list()))   
qty_printers_notif_filtered_under_scope = len(set(notif_filtered_and_under_scope['Sup.Equip. Serial Nb'].to_list()))                      
serie = ['Qty of Printers in notif file', qty_printers_notif, qty_printers_notif_filtered, qty_printers_notif_filtered_under_scope]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)

####Add quantity of notif wo cs order
notif_wo_cs_qty = len(notif[notif['CS Order ID']=='#'])
notif_wo_cs_qty_filters = len(notif_filtered[notif_filtered['CS Order ID']=='#'])
serie = ['Qty of Notifications Wo CS order', notif_wo_cs_qty, notif_wo_cs_qty_filters, np.NaN]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)

########################Metrcis related to notif############################ 
####Sales Channel = Mi Direct
serie = ['Qty Of Notification : Sales Channel = Mi Direct',
         len(notif[notif['Sales Channel']=='MI_Direct']), 
         len(notif_filtered[notif_filtered['Sales Channel']=='MI_Direct']),
         len(notif_filtered_and_under_scope[notif_filtered_and_under_scope['Sales Channel']=='MI_Direct'])]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
####Sales Channel = International_Partners
serie = ['Qty Of Notification : Sales Channel = International_Partners',
         len(notif[notif['Sales Channel']=='International_Partners']),
         len(notif_filtered[notif_filtered['Sales Channel']=='International_Partners']), 
         len(notif_filtered_and_under_scope[notif_filtered_and_under_scope['Sales Channel']=='International_Partners'])]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
####Sales Channel = Local_Partners
serie = ['Qty Of Notification : Sales Channel = Local_Partners',
         len(notif[notif['Sales Channel']=='Local_Partners']), 
         len(notif_filtered[notif_filtered['Sales Channel']=='Local_Partners']), 
         len(notif_filtered_and_under_scope[notif_filtered_and_under_scope['Sales Channel']=='Local_Partners'])]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)



### Origin Flow = AES
serie = ['Qty Of Notification : Origin Flow = AES',
         len(notif[notif['Notif. Origin Flow']=='AES']), 
         len(notif_filtered[notif_filtered['Notif. Origin Flow']=='AES']), 
         len(notif_filtered_and_under_scope[notif_filtered_and_under_scope['Notif. Origin Flow']=='AES'])]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)

### Origin Flow = Help Desk
serie = ['Qty Of Notification : Origin Flow = Help Desk',
         len(notif[notif['Notif. Origin Flow']=='HELPDESK']), 
         len(notif_filtered[notif_filtered['Notif. Origin Flow']=='HELPDESK']), 
         len(notif_filtered_and_under_scope[notif_filtered_and_under_scope['Notif. Origin Flow']=='HELPDESK'])]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)

### Origin Flow = ON SITE
serie = ['Qty Of Notification : Origin Flow = ON SITE',
         len(notif[notif['Notif. Origin Flow']=='ON SITE']), 
         len(notif_filtered[notif_filtered['Notif. Origin Flow']=='ON SITE']), 
         len(notif_filtered_and_under_scope[notif_filtered_and_under_scope['Notif. Origin Flow']=='ON SITE'])]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)


### Origin Flow = OTHERS
serie = ['Qty Of Notification : Origin Flow = OTHERS',
         len(notif[notif['Notif. Origin Flow']=='OTHERS']), 
         len(notif_filtered[notif_filtered['Notif. Origin Flow']=='OTHERS']), 
         len(notif_filtered_and_under_scope[notif_filtered_and_under_scope['Notif. Origin Flow']=='OTHERS'])]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)

##########################Metrics related to cs orders ######################
###MI_Direct
cs_orders_qty_MI_Direct = len(notif[(notif['Sup.Equip. Serial Nb'].isin(IB[IB['Sales Channel'].isin(sales_channel)]['Equipment'].unique()))&(notif['Sales Channel']=='MI_Direct')]['CS Order ID'].unique())
cs_orders_qty_MI_Direct_filtered = len(notif_filtered[(notif_filtered['Sup.Equip. Serial Nb'].isin(IB[IB['Sales Channel'].isin(sales_channel)]['Equipment'].unique()))&(notif_filtered['Sales Channel']=='MI_Direct')]['CS Order ID'].unique())
cs_orders_qty_MI_Direct_filtered_underscope = len(notif_filtered_and_under_scope[(notif_filtered_and_under_scope['Sup.Equip. Serial Nb'].isin(IB[IB['Sales Channel'].isin(sales_channel)]['Equipment'].unique()))&(notif_filtered_and_under_scope['Sales Channel']=='MI_Direct')]['CS Order ID'].unique())
serie = ['Qty of CS order include in Mi Direct (IB & Notif)',
         cs_orders_qty_MI_Direct, cs_orders_qty_MI_Direct_filtered, cs_orders_qty_MI_Direct_filtered_underscope]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
###MI_Direct and sub codes
cs_orders_qty_MI_Direct_sub_codes = len(notif[(notif['Sup.Equip. Serial Nb'].isin(IB[IB['Sales Channel'].isin(sales_channel)]['Equipment'].unique()))&\
                                              (notif['Sales Channel']=='MI_Direct')&\
                                              (notif['Maint. Activity Type'].isin(sub_codes))]['CS Order ID'].unique())
cs_orders_qty_MI_Direct_sub_codes_filtered = len(notif_filtered[(notif_filtered['Sup.Equip. Serial Nb'].isin(IB[IB['Sales Channel'].isin(sales_channel)]['Equipment'].unique()))]['CS Order ID'].unique())
cs_orders_qty_MI_Direct_sub_codes_underscope = len(notif_filtered_and_under_scope[(notif_filtered_and_under_scope['Sup.Equip. Serial Nb'].isin(IB[IB['Sales Channel'].isin(sales_channel)]['Equipment'].unique()))]['CS Order ID'].unique())
serie = ['Qty of CS order include in Mi Direct & SubCode Scope',
         cs_orders_qty_MI_Direct_sub_codes, cs_orders_qty_MI_Direct_sub_codes_filtered, cs_orders_qty_MI_Direct_sub_codes_underscope]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)

#######################Metrics related to global IB#########################
###Qty of Printers in Global IB : Sales Channel = International_Partners
qty_printers_International_Partners = len(set(IB[IB['Sales Channel']=='International_Partners']['Equipment'].to_list()))
serie = ['Qty of Printers in Global IB : Sales Channel = International_Partners',
         qty_printers_International_Partners, np.NaN, np.NaN]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
###Qty of Printers in Global IB : Sales Channel = Local_Partners
qty_printers_Local_Partners = len(set(IB[IB['Sales Channel']=='Local_Partners']['Equipment'].to_list()))
serie = ['Qty of Printers in Global IB : Sales Channel = Local_Partners',
         qty_printers_Local_Partners, np.NaN, np.NaN]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
###Qty of Printers in Global IB : Sales Channel = Mi Direct
qty_printers_MI_Direct = len(set(IB[IB['Sales Channel']=='MI_Direct']['Equipment'].to_list()))
serie = ['Qty of Printers in Global IB : Sales Channel = Mi Direct',
         qty_printers_MI_Direct, np.NaN, np.NaN]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
####Qty of Printers in Global IB : Sales Channel = Mi Direct & In Ship To classification Scope ( 10/11/15/16/51/52/53/98)
qty_printers_MI_Direct_ship_to = len(set(IB[(IB['Sales Channel']=='MI_Direct')&\
                                            (IB['Ship-to Classification'].isin(ship_to_classification))]['Equipment'].to_list()))
serie = ['Qty of Printers in Global IB : Sales Channel = Mi Direct & In Ship To classification Scope',
         qty_printers_MI_Direct_ship_to, np.NaN, np.NaN]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
####Qty of Printers in Global IB : Sales Channel = Mi Direct & In Ship To classification Scope ( 10/11/15/16/51/52/53/98) & Sales Org Filters applay
qty_printers_MI_Direct_ship_to_sales_org = len(set(IB[(IB['Sales Channel']=='MI_Direct')&\
                                            (IB['Ship-to Classification'].isin(ship_to_classification))&\
                                                (IB['Sales Organization'].isin(sales_org ))]['Equipment'].to_list()))
serie = ['Qty of Printers in Global IB : Sales Channel = Mi Direct & In Ship To classification Scope & Sales Org Filters applay',
         qty_printers_MI_Direct_ship_to_sales_org, np.NaN, np.NaN]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
####Qty of Printers in Global IB : Sales Channel = Mi Direct & In Ship To classification Scope ( 10/11/15/16/51/52/53/98) & Sales Org Filters applay & Family Filters apply
qty_printers_MI_Direct_ship_to_sales_org_family = len(set(IB[(IB['Sales Channel']=='MI_Direct')&\
                                            (IB['Ship-to Classification'].isin(ship_to_classification))&\
                                                (IB['Sales Organization'].isin(sales_org ))&\
                                                    (IB['Ind. family - lev. 1'].isin(sectors_filter))]['Equipment'].to_list()))
serie = ['Qty of Printers in Global IB : Sales Channel = Mi Direct & In Ship To classification Scope & Sales Org Filters applay & Family Filters apply',
         qty_printers_MI_Direct_ship_to_sales_org_family, np.NaN, np.NaN]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)


####Qty of Sales Organization # (No Filters)
serie = ['Qty of Sales Organization # (No Filters)',
         len(IB[IB['Sales Organization']=='#']), np.NaN, np.NaN]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)

###Ship to country not assigned
serie = ['Ship to country not assigned',
         len(IB[IB['Ship-to Country']=='#']), np.NaN, np.NaN]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)




#######  GLOBAL IB : Installed at exctraction Time Filters apply (Mi direct)###########
###Qty of Printers in Global IB MI Direct : Status = INST
serie = ['Qty of Printers in Global IB MI Direct & Status = INST',
         len(IB[(IB['Sales Channel'].isin(sales_channel))&(IB['Number \nof equipment\n(INST)']==1)]), 
         len(IB_filtered[(IB_filtered['Sales Channel'].isin(sales_channel))&(IB_filtered['Number \nof equipment\n(INST)']==1)]), np.NaN]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
###Qty of Printers in Global IB MI Direct Under contract
serie = ['Qty of Printers in Global IB MI Direct & Under contract',
         len(IB[(IB['Sales Channel'].isin(sales_channel))&(IB['Number \nof equipment\n(INST)']==1)&(IB['Contract Status']=='Under Contract')]), 
         len(IB_filtered[(IB_filtered['Sales Channel'].isin(sales_channel))&(IB_filtered['Number \nof equipment\n(INST)']==1)&(IB_filtered['Contract Status']=='Under Contract')]), np.NaN]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
###Qty of Printers in Global IB MI Direct Under contract: Standard
serie = ['Qty of Printers in Global IB MI Direct & Under contract: Standard',
         len(IB[(IB['Sales Channel'].isin(sales_channel))&(IB['Number \nof equipment\n(INST)']==1)&(IB['Contract Status']=='Under Contract')&(IB['Contract Nature']=='Standard')]), 
         len(IB_filtered[(IB_filtered['Sales Channel'].isin(sales_channel))&(IB_filtered['Number \nof equipment\n(INST)']==1)&(IB_filtered['Contract Status']=='Under Contract')&(IB_filtered['Contract Nature']=='Standard')]), np.NaN]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
###Qty of Printers in Global IB MI Direct Under contract: Premium
serie = ['Qty of Printers in Global IB MI Direct & Under contract: Premium',
         len(IB[(IB['Sales Channel'].isin(sales_channel))&(IB['Number \nof equipment\n(INST)']==1)&(IB['Contract Status']=='Under Contract')&(IB['Contract Nature']=='Premium')]), 
         len(IB_filtered[(IB_filtered['Sales Channel'].isin(sales_channel))&(IB_filtered['Number \nof equipment\n(INST)']==1)&(IB_filtered['Contract Status']=='Under Contract')&(IB_filtered['Contract Nature']=='Premium')]), np.NaN]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
###Qty of Printers in Global IB MI Direct Under contract: Access
serie = ['Qty of Printers in Global IB MI Direct & Under contract: Access',
         len(IB[(IB['Sales Channel'].isin(sales_channel))&(IB['Number \nof equipment\n(INST)']==1)&(IB['Contract Status']=='Under Contract')&(IB['Contract Nature']=='Access')]), 
         len(IB_filtered[(IB_filtered['Sales Channel'].isin(sales_channel))&(IB_filtered['Number \nof equipment\n(INST)']==1)&(IB_filtered['Contract Status']=='Under Contract')&(IB_filtered['Contract Nature']=='Access')]), np.NaN]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
###Qty of Printers in Global IB MI Direct Under contract: Warranty Extension
serie = ['Qty of Printers in Global IB MI Direct & Under contract: Warranty Extension',
         len(IB[(IB['Sales Channel'].isin(sales_channel))&(IB['Number \nof equipment\n(INST)']==1)&(IB['Contract Status']=='Under Contract')&(IB['Contract Nature']=='Warranty Extension')]), 
         len(IB_filtered[(IB_filtered['Sales Channel'].isin(sales_channel))&(IB_filtered['Number \nof equipment\n(INST)']==1)&(IB_filtered['Contract Status']=='Under Contract')&(IB_filtered['Contract Nature']=='Warranty Extension')]), np.NaN]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
###Qty of Printers in Global IB MI Direct Under contract: Rental
serie = ['Qty of Printers in Global IB MI Direct & Under contract: Rental',
         len(IB[(IB['Sales Channel'].isin(sales_channel))&(IB['Number \nof equipment\n(INST)']==1)&(IB['Contract Status']=='Under Contract')&(IB['Contract Nature']=='Rental')]), 
         len(IB_filtered[(IB_filtered['Sales Channel'].isin(sales_channel))&(IB_filtered['Number \nof equipment\n(INST)']==1)&(IB_filtered['Contract Status']=='Under Contract')&(IB_filtered['Contract Nature']=='Rental')]), np.NaN]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
###Qty of Printers in Global IB MI Direct Under contract: Not assigned
serie = ['Qty of Printers in Global IB MI Direct & Under contract: Not assigned',
         len(IB[(IB['Sales Channel'].isin(sales_channel))&(IB['Number \nof equipment\n(INST)']==1)&(IB['Contract Status']=='Under Contract')&(IB['Contract Nature']=='Not assigned')]), 
         len(IB_filtered[(IB['Sales Channel'].isin(sales_channel))&(IB_filtered['Number \nof equipment\n(INST)']==1)&(IB_filtered['Contract Status']=='Under Contract')&(IB_filtered['Contract Nature']=='Not assigned')]), np.NaN]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)



################################Cs orders#######################################
###qty cs order with sap mvt and qty==0
cs_orders_qty_wo_parts_changed_sap = len(notif[notif['Quantity']==0]['CS Order ID'].unique())
cs_orders_qty_wo_parts_changed_sap_filters = len(notif_filtered[notif_filtered['Quantity']==0]['CS Order ID'].unique())
cs_orders_qty_wo_parts_changed_sap_filters_under_scope = len(notif_filtered_and_under_scope[notif_filtered_and_under_scope['Quantity']==0]['CS Order ID'].unique())
serie = ['Qty of cs orders tracked in SAP with quantity = 0',
         cs_orders_qty_wo_parts_changed_sap, cs_orders_qty_wo_parts_changed_sap_filters,
         cs_orders_qty_wo_parts_changed_sap_filters_under_scope]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)

####Qty cs orders without sap mvt
cs_orders_qty_wo_sap = len(notif[~notif['CS Order ID'].isin(sap_mvt['Order'].unique())]['CS Order ID'].unique())
cs_orders_qty_wo_sap_filters = len(notif_filtered[~notif_filtered['CS Order ID'].isin(sap_mvt['Order'].unique())]['CS Order ID'].unique())
cs_orders_qty_wo_sap_filters_under_scope = len(notif_filtered_and_under_scope[~notif_filtered_and_under_scope['CS Order ID'].isin(sap_mvt['Order'].unique())]['CS Order ID'].unique())
serie = ['Qty of cs orders not tracked in SAP',
         cs_orders_qty_wo_sap, cs_orders_qty_wo_sap_filters,
         cs_orders_qty_wo_sap_filters_under_scope]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)


####Qty cs orders without sap mvt but replaced part in activity
cs_orders_qty_wo_sap_w_replaced_part = len(notif[(~notif['CS Order ID'].isin(sap_mvt['Order'].unique()))&(notif['Activity']=='Replaced part')]['CS Order ID'].unique())
cs_orders_qty_wo_sap_w_replaced_part_filters = len(notif_filtered[(~notif_filtered['CS Order ID'].isin(sap_mvt['Order'].unique()))&(notif_filtered['Activity']=='Replaced part')]['CS Order ID'].unique())
cs_orders_qty_wo_sap_w_replaced_part_filters_under_scope = len(notif_filtered_and_under_scope[(~notif_filtered_and_under_scope['CS Order ID'].isin(sap_mvt['Order'].unique()))&(notif_filtered_and_under_scope['Activity']=='Replaced part')]['CS Order ID'].unique())
serie = ['Qty of cs orders not tracked in SAP but replaced part in activity',
         cs_orders_qty_wo_sap_w_replaced_part, cs_orders_qty_wo_sap_w_replaced_part_filters,
         cs_orders_qty_wo_sap_w_replaced_part_filters_under_scope]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)



####Percentage qty cs orders without sap mvt but replaced part in activity
prct_cs_orders_qty_wo_sap_w_replaced_part = np.around(cs_orders_qty_wo_sap_w_replaced_part*100/cs_orders_qty_wo_sap,2)
prct_cs_orders_qty_wo_sap_w_replaced_part_filters = np.around(cs_orders_qty_wo_sap_w_replaced_part_filters*100/cs_orders_qty_wo_sap_filters,2)
prct_cs_orders_qty_wo_sap_w_replaced_part_filters_under_scope = np.around(cs_orders_qty_wo_sap_w_replaced_part_filters_under_scope*100/cs_orders_qty_wo_sap_filters_under_scope,2)
serie = ['Percentage of qty of cs orders not tracked in SAP but replaced part in activity',
         prct_cs_orders_qty_wo_sap_w_replaced_part, prct_cs_orders_qty_wo_sap_w_replaced_part_filters,
         prct_cs_orders_qty_wo_sap_w_replaced_part_filters_under_scope]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)



#######################Cs orders with 1 part changed################################
###Qty Of Cs Order With 1 parts changed and qty =1
qty_cs_1_part_changed = len(notif[(notif['Quantity']==1)&(notif['Number of parts changed']==1)]['CS Order ID'].unique())
qty_cs_1_part_changed_filters = len(notif_filtered[(notif_filtered['Quantity']==1)&(notif_filtered['Number of parts changed']==1)]['CS Order ID'].unique())
qty_cs_1_part_changed_filters_under_scope = len(notif_filtered_and_under_scope[(notif_filtered_and_under_scope['Quantity']==1)&(notif_filtered_and_under_scope['Number of parts changed']==1)]['CS Order ID'].unique())
serie = ['Qty Of Cs Order With only 1 parts changed and quantity = 1',
         qty_cs_1_part_changed, qty_cs_1_part_changed_filters,
         qty_cs_1_part_changed_filters_under_scope]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)

###Qty Of Cs Order With only 1 parts changed and quantity <1
qty_cs_less_1_part_changed = len(notif[(notif['Quantity']>0)&(notif['Quantity']<1)&(notif['Number of parts changed']==1)]['CS Order ID'].unique())
qty_cs_less_1_part_changed_filters = len(notif_filtered[(notif_filtered['Quantity']>0)&(notif_filtered['Quantity']<1)&(notif_filtered['Number of parts changed']==1)]['CS Order ID'].unique())
qty_cs_less_1_part_changed_filters_under_scope = len(notif_filtered_and_under_scope[(notif_filtered_and_under_scope['Quantity']>0)&(notif_filtered_and_under_scope['Quantity']<1)&(notif_filtered_and_under_scope['Number of parts changed']==1)]['CS Order ID'].unique())
serie = ['Qty Of Cs Order With only 1 parts changed and quantity <1',
         qty_cs_less_1_part_changed, qty_cs_less_1_part_changed_filters,
         qty_cs_less_1_part_changed_filters_under_scope]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)

###Qty Of Cs Order With more than one part changed or qty>1 for same part
qty_cs_more_than_1_part_changed = len(notif[(notif['Number of parts changed']>1)]['CS Order ID'].unique()) + len(notif[(notif['Quantity']>1)&(notif['Number of parts changed']==1)]['CS Order ID'].unique())
qty_cs_more_than_1_part_changed_filters = len(notif_filtered[(notif_filtered['Number of parts changed']>1)]['CS Order ID'].unique()) + len(notif_filtered[(notif_filtered['Quantity']>1)&(notif_filtered['Number of parts changed']==1)]['CS Order ID'].unique())
qty_cs_more_than_1_part_changed_filters_under_scope = len(notif_filtered_and_under_scope[(notif_filtered_and_under_scope['Number of parts changed']>1)]['CS Order ID'].unique()) + len(notif_filtered_and_under_scope[(notif_filtered_and_under_scope['Quantity']>1)&(notif_filtered_and_under_scope['Number of parts changed']==1)]['CS Order ID'].unique())
serie = ['Qty Of Cs Order With more than one part changed or qty>1 for same part',
         qty_cs_more_than_1_part_changed, qty_cs_more_than_1_part_changed_filters,
         qty_cs_more_than_1_part_changed_filters_under_scope]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)

# #####
# cs_orders_greater_1 = sum((list(notif[(notif['Quantity']>1)&(notif['Number of parts changed']==1)]['CS Order ID'].unique()), list(notif[(notif['Number of parts changed']>1)]['CS Order ID'].unique())), [])
# cs_orders_equal_1 = list(notif[(notif['Quantity']==1)&(notif['Number of parts changed']==1)]['CS Order ID'].unique())
# cs_orders_with_rep =  list(notif[notif['Quantity']>0]['CS Order ID'].unique())
# missed_cs = [x for x in cs_orders_with_rep if (x not in cs_orders_greater_1) and (x not in cs_orders_equal_1) ]
# ex = notif[notif['CS Order ID'].isin(missed_cs)]

###Qty Of cs Order with quantity >1 for the same component

print('-------------------------------------')
print('Last steps')
qty_notif_more_than_1_part_changed_same_component = 0
for i, j in enumerate(tqdm(notif['Notif. ID'].values)):
    if type(notif.iloc[i]['Parts changed'])==dict:
        if any(list(map(lambda x: x>1, list(notif.iloc[i]['Parts changed'].values())))):
            qty_notif_more_than_1_part_changed_same_component+=1

qty_notif_more_than_1_part_changed_same_component_filtered = 0
for i, j in enumerate(tqdm(notif_filtered['Notif. ID'].values)):
    if type(notif_filtered.iloc[i]['Parts changed'])==dict:
        if any(list(map(lambda x: x>1, list(notif_filtered.iloc[i]['Parts changed'].values())))):
            qty_notif_more_than_1_part_changed_same_component_filtered+=1

qty_notif_more_than_1_part_changed_same_component_filtered_and_under_scope = 0
for i, j in enumerate(tqdm(notif_filtered_and_under_scope['Notif. ID'].values)):
    if type(notif_filtered_and_under_scope.iloc[i]['Parts changed'])==dict:
        if any(list(map(lambda x: x>1, list(notif_filtered_and_under_scope.iloc[i]['Parts changed'].values())))):
            qty_notif_more_than_1_part_changed_same_component_filtered_and_under_scope+=1
serie = ['Qty Of cs Order with quantity >1 for the same component',
         qty_notif_more_than_1_part_changed_same_component, qty_notif_more_than_1_part_changed_same_component_filtered,
         qty_notif_more_than_1_part_changed_same_component_filtered_and_under_scope]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)

##########################Qty material changed################################
qty_material_changed = notif['Quantity'].sum()
qty_material_changed_filters = notif_filtered['Quantity'].sum()
qty_material_changed_filters_under_scope = notif_filtered_and_under_scope['Quantity'].sum()
serie = ['Qty of Total Material Changed',
         qty_material_changed, qty_material_changed_filters,
         qty_material_changed_filters_under_scope]
metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)



type_printer = '9410'
type_printer = '9018-28-29-9330'
metrics.to_excel( r'C:\Users\mjmel\Desktop\Internship\Metrics\metrics_{}_{}.xlsx'.format(type_printer, str(date.today())), index = False)



# # (notif_filtered['Quantity'].sum()-notif_filtered['Number of parts changed'].sum())*100/notif_filtered['Quantity'].sum()

# len(notif_filtered[notif_filtered['Quantity']==notif_filtered['Number of parts changed']])*100/len(notif_filtered)
