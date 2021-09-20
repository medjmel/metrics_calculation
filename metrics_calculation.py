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

def format_tbl(writer, sheet_name, df):
    outcols = df.columns
    if len(outcols) > 25:
        raise ValueError('table width out of range for current logic')
    tbl_hdr = [{'header':c} for c in outcols]
    bottom_num = len(df)+1
    right_letter = chr(65-1+len(outcols))
    tbl_corner = right_letter + str(bottom_num)

    worksheet = writer.sheets[sheet_name]
    worksheet.add_table('A1:' + tbl_corner,  {'columns':tbl_hdr})


def calculate_metrics(notif_path, IB_path, contracts_path, sap_mvt_path, printer_type, excel_output_path):
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
    reliability_sub_codes = ['010', '100', '120', '130', '140', '145', '150', '155', '160', '165',
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
    
    notif_filtered = notif[(notif['Maint. Activity Type'].isin(reliability_sub_codes))&\
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
    
    
    
    notif = notif.drop_duplicates(subset=['Notif. ID'])
    notif_filtered = notif_filtered.drop_duplicates(subset=['Notif. ID'])
    notif_filtered_and_under_scope = notif_filtered_and_under_scope.drop_duplicates(subset=['Notif. ID'])
    
    
    #####Calculate metrics
    metrics = pd.DataFrame(columns = ['Metric', 'How', 'Value wo filters',
                                      'Value with filters', 'Value with filters and under scope DDSO'])
    
    ########################################################################
    ########################################################################
    ############################# Block 6 ##################################
    ########################################################################
    ########################################################################
    #######################Metrics related to global IB#########################
    ###Qty of Printers in Global IB : Sales Channel = International_Partners
    qty_printers_International_Partners = len(set(IB[IB['Sales Channel']=='International_Partners']['Equipment'].to_list()))
    serie = ['Number of International Partners sales',
             'Count distinct Equipment from IB file where: Sales Channel = International_Partners',
             qty_printers_International_Partners,
             np.NaN, 
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ###Qty of Printers in Global IB : Sales Channel = Local_Partners
    qty_printers_Local_Partners = len(set(IB[IB['Sales Channel']=='Local_Partners']['Equipment'].to_list()))
    serie = ['Number of Local Partners sales',
             'Count distinct Equipment from IB file where: Sales Channel = Local_Partners',
             qty_printers_Local_Partners,
             np.NaN,
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ###Qty of Printers in Global IB : Sales Channel = Mi Direct
    qty_printers_MI_Direct = len(set(IB[IB['Sales Channel']=='MI_Direct']['Equipment'].to_list()))
    serie = ['Number of MI Direct sales',
             'Count distinct Equipment from IB file where: Sales Channel = MI_Direct',
             qty_printers_MI_Direct, 
             np.NaN, 
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ########################################################################
    ########################################################################
    ############################# Block 4 ##################################
    ########################################################################
    ########################################################################
    ########################Metrcis related to notif############################ 
    ####Sales Channel = Mi Direct
    serie = ['Number of MI Direct notifications',
             'Count notifications from notifications file where: Sales Channel = Mi Direct',
             len(notif[notif['Sales Channel']=='MI_Direct']), 
             len(notif_filtered[notif_filtered['Sales Channel']=='MI_Direct']),
             len(notif_filtered_and_under_scope[notif_filtered_and_under_scope['Sales Channel']=='MI_Direct'])]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    ####Sales Channel = International_Partners
    serie = ['Number of International Partners notifications',
             'Count notifications from notifications file where: Sales Channel = International_Partners',
             len(notif[notif['Sales Channel']=='International_Partners']),
             len(notif_filtered[notif_filtered['Sales Channel']=='International_Partners']), 
             len(notif_filtered_and_under_scope[notif_filtered_and_under_scope['Sales Channel']=='International_Partners'])]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    ####Sales Channel = Local_Partners
    serie = ['Number of Local Partners notifications',
             'Count notifications from notifications file where: Sales Channel = Local_Partners',
             len(notif[notif['Sales Channel']=='Local_Partners']), 
             len(notif_filtered[notif_filtered['Sales Channel']=='Local_Partners']), 
             len(notif_filtered_and_under_scope[notif_filtered_and_under_scope['Sales Channel']=='Local_Partners'])]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    
    
    
    ########################################################################
    ########################################################################
    ############################# Block 7 ##################################
    ########################################################################
    ########################################################################
    ####Qty of Printers in Global IB : Sales Channel = Mi Direct & In Ship To classification Scope ( 10/11/15/16/51/52/53/98)
    qty_printers_MI_Direct_ship_to = len(set(IB[(IB['Sales Channel']=='MI_Direct')&\
                                                (IB['Ship-to Classification'].isin(ship_to_classification))]['Equipment'].to_list()))
    serie = ['Number of MI Direct printers in IB belonging to Ship-to Classification scope',
             'Count disctint Equipment from Global IB where: Sales Channel = Mi Direct & Ship-to Classification belongs to Scope',
             qty_printers_MI_Direct_ship_to,
             np.NaN,
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ####Qty of Printers in Global IB : Sales Channel = Mi Direct & In Ship To classification Scope ( 10/11/15/16/51/52/53/98) & Sales Org Filters applay
    qty_printers_MI_Direct_ship_to_sales_org = len(set(IB[(IB['Sales Channel']=='MI_Direct')&\
                                                (IB['Ship-to Classification'].isin(ship_to_classification))&\
                                                    (IB['Sales Organization'].isin(sales_org ))]['Equipment'].to_list()))
    serie = ['Number of MI Direct printers in IB belonging to Ship-to Classification and Sales Organization scope',
             'Count disctint Equipment from Global IB where: Sales Channel = Mi Direct & Ship-to Classification belongs to Scope & Sales Org belongs to scope',
             qty_printers_MI_Direct_ship_to_sales_org,
             np.NaN,
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ####Qty of Printers in Global IB : Sales Channel = Mi Direct & In Ship To classification Scope ( 10/11/15/16/51/52/53/98) & Sales Org Filters applay & Family Filters apply
    qty_printers_MI_Direct_ship_to_sales_org_family = len(set(IB[(IB['Sales Channel']=='MI_Direct')&\
                                                (IB['Ship-to Classification'].isin(ship_to_classification))&\
                                                    (IB['Sales Organization'].isin(sales_org ))&\
                                                        (IB['Ind. family - lev. 1'].isin(sectors_filter))]['Equipment'].to_list()))
    serie = ['Number of MI Direct printers in IB belonging to Ship-to Classification, Sales Organization and Sector scope',
             'Count disctint Equipment from Global IB where: Sales Channel = Mi Direct & Ship-to Classification belongs to Scope & Sales Org belongs to scope & Family belongs to scope',
             qty_printers_MI_Direct_ship_to_sales_org_family, 
             np.NaN,
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    
    
    
    ########################################################################
    ########################################################################
    ############################# Block 8 ##################################
    ########################################################################
    ########################################################################
    ####Qty of Sales Organization # (No Filters)
    serie = ['Number of printers with sales organization not specified',
             'Count printers from IB file where: Sales Organization = #',
             len(IB[IB['Sales Organization']=='#']),
             np.NaN,
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ###Ship to country not assigned
    serie = ['Number of printers with country not specified',
             'Count printers from IB file where: Ship-to Country = #',
             len(IB[IB['Ship-to Country']=='#']),
             np.NaN,
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    
    
    
    
    
    ########################################################################
    ########################################################################
    ############################# Block 1 ##################################
    ########################################################################
    ########################################################################
    ####Add number of notifications 
    notif_qty = len(notif)
    notif_qty_filters = len(notif_filtered)
    notif_qty_filters_under_scope = len(notif_filtered_and_under_scope)
    serie = ['Number of SAP notifications',
             'Count of notifications from notifications file',
             notif_qty,
             notif_qty_filters,
             notif_qty_filters_under_scope]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ### Origin Flow = ON SITE
    serie = ['Number of ON SITE notifications',
             'Count of notifications where: Origin Flow = ON SITE',
             len(notif[notif['Notif. Origin Flow']=='ON SITE']), 
             len(notif_filtered[notif_filtered['Notif. Origin Flow']=='ON SITE']), 
             len(notif_filtered_and_under_scope[notif_filtered_and_under_scope['Notif. Origin Flow']=='ON SITE'])]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ### Origin Flow = OTHERS
    serie = ['Number of OTHERS notifications',
             'Count of notifications where: Origin Flow = OTHERS',
             len(notif[notif['Notif. Origin Flow']=='OTHERS']), 
             len(notif_filtered[notif_filtered['Notif. Origin Flow']=='OTHERS']), 
             len(notif_filtered_and_under_scope[notif_filtered_and_under_scope['Notif. Origin Flow']=='OTHERS'])]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ### Origin Flow = AES
    serie = ['Number of AES notifications',
             'Count of notifications where: Origin Flow = AES',
             len(notif[notif['Notif. Origin Flow']=='AES']), 
             len(notif_filtered[notif_filtered['Notif. Origin Flow']=='AES']), 
             len(notif_filtered_and_under_scope[notif_filtered_and_under_scope['Notif. Origin Flow']=='AES'])]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ### Origin Flow = Help Desk
    serie = ['Number of Help Desk notifications method',
             'Count of notifications where: Origin Flow = Help Desk',
             len(notif[notif['Notif. Origin Flow']=='HELPDESK']), 
             len(notif_filtered[notif_filtered['Notif. Origin Flow']=='HELPDESK']), 
             len(notif_filtered_and_under_scope[notif_filtered_and_under_scope['Notif. Origin Flow']=='HELPDESK'])]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    
    
    
    ########################################################################
    ########################################################################
    ############################# Block 2 ##################################
    ########################################################################
    ########################################################################
    ####Add quantity of notif wo cs order
    notif_wo_cs_qty = len(notif[notif['CS Order ID']=='#'])
    notif_wo_cs_qty_filters = len(notif_filtered[notif_filtered['CS Order ID']=='#'])
    serie = ['Number of notifications without field visit',
             'Count of notifications where CS Order ID == #',
             notif_wo_cs_qty,
             notif_wo_cs_qty_filters,
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ####Add quantity of notif with cs order
    notif_with_cs_qty = len(notif[notif['CS Order ID']!='#'])
    notif_with_cs_qty_filters = len(notif_filtered[notif_filtered['CS Order ID']!='#'])
    serie = ['Number of notifications with field visit',
             'Count of notifications where CS Order ID != #',
             notif_with_cs_qty,
             notif_with_cs_qty_filters,
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    
    ########################################################################
    ########################################################################
    ############################# Block 2 ##################################
    ########################################################################
    ########################################################################
    ###Add number of cs orders
    cs_orders_qty = len(notif['CS Order ID'].unique())
    cs_orders_qty_filters = len(notif_filtered['CS Order ID'].unique())
    cs_orders_qty_filters_under_scope = len(notif_filtered_and_under_scope['CS Order ID'].unique())
    serie = ['Number of field interventions',
             'Count distinct CS Order ID from notifications file',
             cs_orders_qty,
             cs_orders_qty_filters,
             cs_orders_qty_filters_under_scope]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ####Qty cs orders without sap mvt
    cs_orders_qty_wo_sap = len(notif[~notif['CS Order ID'].isin(sap_mvt['Order'].unique())]['CS Order ID'].unique())
    cs_orders_qty_wo_sap_filters = len(notif_filtered[~notif_filtered['CS Order ID'].isin(sap_mvt['Order'].unique())]['CS Order ID'].unique())
    cs_orders_qty_wo_sap_filters_under_scope = len(notif_filtered_and_under_scope[~notif_filtered_and_under_scope['CS Order ID'].isin(sap_mvt['Order'].unique())]['CS Order ID'].unique())
    serie = ['Number of field interventions without part replacement',
             'Count distinct CS Order ID from notifications file where CS Order ID not in SAP mvt',
             cs_orders_qty_wo_sap, cs_orders_qty_wo_sap_filters,
             cs_orders_qty_wo_sap_filters_under_scope]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    
    ###Add number of cs orders with part changement
    cs_orders_qty_w_parts_changed = len(notif[notif['Quantity']>0]['CS Order ID'].unique())
    cs_orders_qty_w_parts_changed_filters = len(notif_filtered[notif_filtered['Quantity']>0]['CS Order ID'].unique())
    cs_orders_qty_w_parts_changed_filters_under_scope = len(notif_filtered_and_under_scope[notif_filtered_and_under_scope['Quantity']>0]['CS Order ID'].unique())
    serie = ['Number of field interventions with parts replacement tracked in SAP mvt',
             'Count distinct CS Order ID from notifications file where the quantity of repalced part from SAP mvt is > 0', 
             cs_orders_qty_w_parts_changed,
             cs_orders_qty_w_parts_changed_filters, 
             cs_orders_qty_w_parts_changed_filters_under_scope]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ###qty cs order with sap mvt and qty==0
    cs_orders_qty_wo_parts_changed_sap = len(notif[notif['Quantity']==0]['CS Order ID'].unique())
    cs_orders_qty_wo_parts_changed_sap_filters = len(notif_filtered[notif_filtered['Quantity']==0]['CS Order ID'].unique())
    cs_orders_qty_wo_parts_changed_sap_filters_under_scope = len(notif_filtered_and_under_scope[notif_filtered_and_under_scope['Quantity']==0]['CS Order ID'].unique())
    serie = ['Number of field interventions tracked in SAP mvt but quantity = 0',
             'Count distinct CS Order ID from notifications file where the quantity of repalced part from SAP mvt is = 0',
             cs_orders_qty_wo_parts_changed_sap, cs_orders_qty_wo_parts_changed_sap_filters,
             cs_orders_qty_wo_parts_changed_sap_filters_under_scope]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    #######Qty notif mention in sap with quantity equal to 0 and with activity part replaced#######
    qty_notif_w_sap_mvt_qty_0_w_replaced_part = len(notif[(notif['Parts changed'].notnull()) &(notif['Quantity']==0) & (notif['Activity']=='Replaced part')])
    qty_notif_w_sap_mvt_qty_0_w_replaced_part_filters = len(notif_filtered[(notif_filtered['Parts changed'].notnull()) &(notif['Quantity']==0) & (notif_filtered['Activity']=='Replaced part')])
    qty_notif_w_sap_mvt_qty_0_w_replaced_part_filters_under_scope = len(notif_filtered_and_under_scope[(notif_filtered_and_under_scope['Parts changed'].notnull()) &(notif['Quantity']==0) & (notif_filtered_and_under_scope['Activity']=='Replaced part')])
    serie = ['Number of field interventions with SAP mvt but quantity equal to 0 and with Activity = Replaced part',
             'Count of notifications from notifications file with SAP mvt but quantity equal to 0 and with Activity = Replaced part',
             qty_notif_w_sap_mvt_qty_0_w_replaced_part, 
             qty_notif_w_sap_mvt_qty_0_w_replaced_part_filters,
             qty_notif_w_sap_mvt_qty_0_w_replaced_part_filters_under_scope]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    
    
    ########################################################################
    ########################################################################
    ############################# Block 2 ##################################
    ########################################################################
    ########################################################################
    #######Qty notif without mention in sap and with activity part replaced#######
    qty_notif_wo_sap_mvt_w_replaced_part = len(notif[(~notif['Parts changed'].notnull()) & (notif['Activity']=='Replaced part')])
    qty_notif_wo_sap_mvt_w_replaced_part_filters = len(notif_filtered[(~notif_filtered['Parts changed'].notnull()) & (notif_filtered['Activity']=='Replaced part')])
    qty_notif_wo_sap_mvt_w_replaced_part_filters_under_scope = len(notif_filtered_and_under_scope[(~notif_filtered_and_under_scope['Parts changed'].notnull()) & (notif_filtered_and_under_scope['Activity']=='Replaced part')])
    serie = ['Number of field interventions with parts replacement not reported in SAP mvt',
             "Count of notifications from notifications file where the notifications don't have an SAP mvt but Activity = Replaced part",
             qty_notif_wo_sap_mvt_w_replaced_part, 
             qty_notif_wo_sap_mvt_w_replaced_part_filters,
             qty_notif_wo_sap_mvt_w_replaced_part_filters_under_scope]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ###Add number of cs orders with part changement
    cs_orders_qty_w_parts_changed = len(notif[notif['Quantity']>0]['CS Order ID'].unique())
    cs_orders_qty_w_parts_changed_filters = len(notif_filtered[notif_filtered['Quantity']>0]['CS Order ID'].unique())
    cs_orders_qty_w_parts_changed_filters_under_scope = len(notif_filtered_and_under_scope[notif_filtered_and_under_scope['Quantity']>0]['CS Order ID'].unique())
    serie = ['Number of field interventions with parts replacement tracked in SAP mvt',
             'Count distinct CS Order ID from notifications file where the quantity of repalced part from SAP mvt is > 0', 
             cs_orders_qty_w_parts_changed,
             cs_orders_qty_w_parts_changed_filters, 
             cs_orders_qty_w_parts_changed_filters_under_scope]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ###Number of interventions with actual replacement
    serie = ['Number of interventions with actual replacement',
             "Count distinct CS Order ID from notifications file where the quantity of repalced part from SAP mvt is > 0 + Count of notifications from notifications file where the notifications don't have an SAP mvt but Activity = Replaced part",
             qty_notif_wo_sap_mvt_w_replaced_part + cs_orders_qty_w_parts_changed, 
             qty_notif_wo_sap_mvt_w_replaced_part_filters + cs_orders_qty_w_parts_changed_filters,
             qty_notif_wo_sap_mvt_w_replaced_part_filters_under_scope + cs_orders_qty_w_parts_changed_filters_under_scope]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    
    
    ########################################################################
    ########################################################################
    ############################# Block 3 ##################################
    ########################################################################
    ########################################################################
    ####Add quantity of printers in notif
    qty_printers_notif = len(set(notif['Sup.Equip. Serial Nb'].to_list()))
    qty_printers_notif_filtered = len(set(notif_filtered['Sup.Equip. Serial Nb'].to_list()))   
    qty_printers_notif_filtered_under_scope = len(set(notif_filtered_and_under_scope['Sup.Equip. Serial Nb'].to_list()))                      
    serie = ['Number of Printers in notifications file',
             'Count distinct Sup.Equip. Serial Nb from notifications file',
             qty_printers_notif,
             qty_printers_notif_filtered, 
             qty_printers_notif_filtered_under_scope]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ####Add quantity of printers
    qty_printers = len(set(IB['Equipment'].to_list()))
    qty_printers_filtered = len(set(IB_filtered['Equipment'].to_list()))   
    qty_printers_filtered_under_scope = len(set(IB_filtered_and_under_scope['Equipment'].to_list()))                      
    serie = ['Number of Printers in Global IB',
             'Count distinct Equipment from IB file', 
             qty_printers, 
             qty_printers_filtered, 
             qty_printers_filtered_under_scope]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    
    ###Qty of Printers in Global IB MI Direct : Status = INST
    serie = ['Number of active printers in Global IB MI Direct',
             'Count of Equipment from Global IB where Sales Channel = MI Direct & Status = INST',
             len(IB[(IB['Sales Channel'].isin(sales_channel))&(IB['Number \nof equipment\n(INST)']==1)]), 
             len(IB_filtered[(IB_filtered['Sales Channel'].isin(sales_channel))&(IB_filtered['Number \nof equipment\n(INST)']==1)]),
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ###Qty of Printers in Global IB MI Direct Under contract
    serie = ['Number of active printers in Global IB MI Direct under contract',
             'Count of Equipment from Global IB where Sales Channel = MI Direct & Status = INST & Contract Status = Under Contract',
             len(IB[(IB['Sales Channel'].isin(sales_channel))&(IB['Number \nof equipment\n(INST)']==1)&(IB['Contract Status']=='Under Contract')]), 
             len(IB_filtered[(IB_filtered['Sales Channel'].isin(sales_channel))&(IB_filtered['Number \nof equipment\n(INST)']==1)&(IB_filtered['Contract Status']=='Under Contract')]),
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ###Qty of Printers in Global IB MI Direct Out of contract
    serie = ['Number of active printers in Global IB MI Direct out of contract',
             'Count of Equipment from Global IB where Sales Channel = MI Direct & Status = INST & Contract Status = Out of Contract',
             len(IB[(IB['Sales Channel'].isin(sales_channel))&(IB['Number \nof equipment\n(INST)']==1)&(IB['Contract Status']=='Out of Contract')]), 
             len(IB_filtered[(IB_filtered['Sales Channel'].isin(sales_channel))&(IB_filtered['Number \nof equipment\n(INST)']==1)&(IB_filtered['Contract Status']=='Out of Contract')]),
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    
    ###Qty of Printers in Global IB MI Direct Under contract: Standard
    serie = ['Number of active printers in Global IB MI Direct under Standard contract',
             'Count of Equipment from Global IB where Sales Channel = MI Direct & Status = INST & Contract Status = Under Contract & Contract Nature = Standard',
             len(IB[(IB['Sales Channel'].isin(sales_channel))&(IB['Number \nof equipment\n(INST)']==1)&(IB['Contract Status']=='Under Contract')&(IB['Contract Nature']=='Standard')]), 
             len(IB_filtered[(IB_filtered['Sales Channel'].isin(sales_channel))&(IB_filtered['Number \nof equipment\n(INST)']==1)&(IB_filtered['Contract Status']=='Under Contract')&(IB_filtered['Contract Nature']=='Standard')]),
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ###Qty of Printers in Global IB MI Direct Under contract: Premium
    serie = ['Number of active printers in Global IB MI Direct under Premium contract',
             'Count of Equipment from Global IB where Sales Channel = MI Direct & Status = INST & Contract Status = Under Contract & Contract Nature = Premium',
             len(IB[(IB['Sales Channel'].isin(sales_channel))&(IB['Number \nof equipment\n(INST)']==1)&(IB['Contract Status']=='Under Contract')&(IB['Contract Nature']=='Premium')]), 
             len(IB_filtered[(IB_filtered['Sales Channel'].isin(sales_channel))&(IB_filtered['Number \nof equipment\n(INST)']==1)&(IB_filtered['Contract Status']=='Under Contract')&(IB_filtered['Contract Nature']=='Premium')]),
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ###Qty of Printers in Global IB MI Direct Under contract: Access
    serie = ['Number of active printers in Global IB MI Direct under Access contract',
             'Count of Equipment from Global IB where Sales Channel = MI Direct & Status = INST & Contract Status = Under Contract & Contract Nature = Access',
             len(IB[(IB['Sales Channel'].isin(sales_channel))&(IB['Number \nof equipment\n(INST)']==1)&(IB['Contract Status']=='Under Contract')&(IB['Contract Nature']=='Access')]), 
             len(IB_filtered[(IB_filtered['Sales Channel'].isin(sales_channel))&(IB_filtered['Number \nof equipment\n(INST)']==1)&(IB_filtered['Contract Status']=='Under Contract')&(IB_filtered['Contract Nature']=='Access')]),
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ###Qty of Printers in Global IB MI Direct Under contract: Warranty Extension
    serie = ['Number of active printers in Global IB MI Direct under Warranty Extension contract',
             'Count of Equipment from Global IB where Sales Channel = MI Direct & Status = INST & Contract Status = Under Contract & Contract Nature = Warranty Extension',
             len(IB[(IB['Sales Channel'].isin(sales_channel))&(IB['Number \nof equipment\n(INST)']==1)&(IB['Contract Status']=='Under Contract')&(IB['Contract Nature']=='Warranty Extension')]), 
             len(IB_filtered[(IB_filtered['Sales Channel'].isin(sales_channel))&(IB_filtered['Number \nof equipment\n(INST)']==1)&(IB_filtered['Contract Status']=='Under Contract')&(IB_filtered['Contract Nature']=='Warranty Extension')]),
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ###Qty of Printers in Global IB MI Direct Under contract: Rental
    serie = ['Number of active printers in Global IB MI Direct under Rental contract',
             'Count of Equipment from Global IB where Sales Channel = MI Direct & Status = INST & Contract Status = Under Contract & Contract Nature = Rental',
             len(IB[(IB['Sales Channel'].isin(sales_channel))&(IB['Number \nof equipment\n(INST)']==1)&(IB['Contract Status']=='Under Contract')&(IB['Contract Nature']=='Rental')]), 
             len(IB_filtered[(IB_filtered['Sales Channel'].isin(sales_channel))&(IB_filtered['Number \nof equipment\n(INST)']==1)&(IB_filtered['Contract Status']=='Under Contract')&(IB_filtered['Contract Nature']=='Rental')]), 
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ###Qty of Printers in Global IB MI Direct Under contract: Not assigned
    serie = ['Number of active printers in Global IB MI Direct under Not assigned contract',
             'Count of Equipment from Global IB where Sales Channel = MI Direct & Status = INST & Contract Status = Under Contract & Contract Nature = Not assigned',
             len(IB[(IB['Sales Channel'].isin(sales_channel))&(IB['Number \nof equipment\n(INST)']==1)&(IB['Contract Status']=='Under Contract')&(IB['Contract Nature']=='Not assigned')]), 
             len(IB_filtered[(IB['Sales Channel'].isin(sales_channel))&(IB_filtered['Number \nof equipment\n(INST)']==1)&(IB_filtered['Contract Status']=='Under Contract')&(IB_filtered['Contract Nature']=='Not assigned')]), np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    ########################################################################
    ########################################################################
    ############################# Block 5 ##################################
    ########################################################################
    ########################################################################
    ##########################Metrics related to cs orders ######################
    ###MI_Direct
    cs_orders_qty_MI_Direct = len(notif[(notif['Sup.Equip. Serial Nb'].isin(IB[IB['Sales Channel'].isin(sales_channel)]['Equipment'].unique()))&(notif['Sales Channel']=='MI_Direct')]['CS Order ID'].unique())
    cs_orders_qty_MI_Direct_filtered = len(notif_filtered[(notif_filtered['Sup.Equip. Serial Nb'].isin(IB[IB['Sales Channel'].isin(sales_channel)]['Equipment'].unique()))&(notif_filtered['Sales Channel']=='MI_Direct')]['CS Order ID'].unique())
    cs_orders_qty_MI_Direct_filtered_underscope = len(notif_filtered_and_under_scope[(notif_filtered_and_under_scope['Sup.Equip. Serial Nb'].isin(IB[IB['Sales Channel'].isin(sales_channel)]['Equipment'].unique()))&(notif_filtered_and_under_scope['Sales Channel']=='MI_Direct')]['CS Order ID'].unique())
    serie = ['Number of CS order included in Mi Direct (IB & Notif)',
             'Count distinct CS Order ID from notifications file where Sup.Equip. Serial Nb is MI_Direct in IB file and MI_Direct in notifications file',
             cs_orders_qty_MI_Direct, 
             cs_orders_qty_MI_Direct_filtered,
             cs_orders_qty_MI_Direct_filtered_underscope]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ###MI_Direct and sub codes
    cs_orders_qty_MI_Direct_reliability_sub_codes = len(notif[(notif['Sup.Equip. Serial Nb'].isin(IB[IB['Sales Channel'].isin(sales_channel)]['Equipment'].unique()))&\
                                                  (notif['Sales Channel']=='MI_Direct')&\
                                                  (notif['Maint. Activity Type'].isin(reliability_sub_codes))]['CS Order ID'].unique())
    cs_orders_qty_MI_Direct_reliability_sub_codes_filtered = len(notif_filtered[(notif_filtered['Sup.Equip. Serial Nb'].isin(IB[IB['Sales Channel'].isin(sales_channel)]['Equipment'].unique()))]['CS Order ID'].unique())
    cs_orders_qty_MI_Direct_reliability_sub_codes_underscope = len(notif_filtered_and_under_scope[(notif_filtered_and_under_scope['Sup.Equip. Serial Nb'].isin(IB[IB['Sales Channel'].isin(sales_channel)]['Equipment'].unique()))]['CS Order ID'].unique())
    serie = ['Number of CS order included in Mi Direct (IB & Notif) & Reliability Scope',
             'Count distinct CS Order ID from notifications file where Sup.Equip. Serial Nb is MI_Direct in IB file and MI_Direct in notifications file & Maint. Activity Type in DDSO subcodes scope',
             cs_orders_qty_MI_Direct_reliability_sub_codes, 
             cs_orders_qty_MI_Direct_reliability_sub_codes_filtered, 
             cs_orders_qty_MI_Direct_reliability_sub_codes_underscope]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    
    
    
    
    
    
    ########################################################################
    ########################################################################
    ############################# Block 6 ##################################
    ########################################################################
    ########################################################################
    #######################Metrics related to global IB#########################
    ###Qty of Printers in Global IB : Sales Channel = International_Partners
    qty_printers_International_Partners = len(set(IB[IB['Sales Channel']=='International_Partners']['Equipment'].to_list()))
    serie = ['Number of International Partners sales',
             'Count distinct Equipment from IB file where: Sales Channel = International_Partners',
             qty_printers_International_Partners,
             np.NaN, 
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ###Qty of Printers in Global IB : Sales Channel = Local_Partners
    qty_printers_Local_Partners = len(set(IB[IB['Sales Channel']=='Local_Partners']['Equipment'].to_list()))
    serie = ['Number of Local Partners sales',
             'Count distinct Equipment from IB file where: Sales Channel = Local_Partners',
             qty_printers_Local_Partners,
             np.NaN,
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ###Qty of Printers in Global IB : Sales Channel = Mi Direct
    qty_printers_MI_Direct = len(set(IB[IB['Sales Channel']=='MI_Direct']['Equipment'].to_list()))
    serie = ['Number of MI Direct sales',
             'Count distinct Equipment from IB file where: Sales Channel = MI_Direct',
             qty_printers_MI_Direct, 
             np.NaN, 
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    
    
    ########################################################################
    ########################################################################
    ############################# Block 7 ##################################
    ########################################################################
    ########################################################################
    ####Qty of Printers in Global IB : Sales Channel = Mi Direct & In Ship To classification Scope ( 10/11/15/16/51/52/53/98)
    qty_printers_MI_Direct_ship_to = len(set(IB[(IB['Sales Channel']=='MI_Direct')&\
                                                (IB['Ship-to Classification'].isin(ship_to_classification))]['Equipment'].to_list()))
    serie = ['Number of MI Direct printers in IB belonging to Ship-to Classification scope',
             'Count disctint Equipment from Global IB where: Sales Channel = Mi Direct & Ship-to Classification belongs to Scope',
             qty_printers_MI_Direct_ship_to,
             np.NaN,
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ####Qty of Printers in Global IB : Sales Channel = Mi Direct & In Ship To classification Scope ( 10/11/15/16/51/52/53/98) & Sales Org Filters applay
    qty_printers_MI_Direct_ship_to_sales_org = len(set(IB[(IB['Sales Channel']=='MI_Direct')&\
                                                (IB['Ship-to Classification'].isin(ship_to_classification))&\
                                                    (IB['Sales Organization'].isin(sales_org ))]['Equipment'].to_list()))
    serie = ['Number of MI Direct printers in IB belonging to Ship-to Classification and Sales Organization scope',
             'Count disctint Equipment from Global IB where: Sales Channel = Mi Direct & Ship-to Classification belongs to Scope & Sales Org belongs to scope',
             qty_printers_MI_Direct_ship_to_sales_org,
             np.NaN,
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ####Qty of Printers in Global IB : Sales Channel = Mi Direct & In Ship To classification Scope ( 10/11/15/16/51/52/53/98) & Sales Org Filters applay & Family Filters apply
    qty_printers_MI_Direct_ship_to_sales_org_family = len(set(IB[(IB['Sales Channel']=='MI_Direct')&\
                                                (IB['Ship-to Classification'].isin(ship_to_classification))&\
                                                    (IB['Sales Organization'].isin(sales_org ))&\
                                                        (IB['Ind. family - lev. 1'].isin(sectors_filter))]['Equipment'].to_list()))
    serie = ['Number of MI Direct printers in IB belonging to Ship-to Classification, Sales Organization and Family scope',
             'Count disctint Equipment from Global IB where: Sales Channel = Mi Direct & Ship-to Classification belongs to Scope & Sales Org belongs to scope & Family belongs to scope',
             qty_printers_MI_Direct_ship_to_sales_org_family, 
             np.NaN,
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    
    
    
    ########################################################################
    ########################################################################
    ############################# Block 8 ##################################
    ########################################################################
    ########################################################################
    ####Qty of Sales Organization # (No Filters)
    serie = ['Number of printers with sales organization not specified',
             'Count printers from IB file where: Sales Organization = #',
             len(IB[IB['Sales Organization']=='#']),
             np.NaN,
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ####Qty of Sales Organization specified
    serie = ['Number of printers with sales organization specified',
             'Count printers from IB file where: Sales Organization != #',
             len(IB[IB['Sales Organization']!='#']),
             np.NaN,
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    ###Ship to country not assigned
    serie = ['Number of printers with country not specified',
             'Count printers from IB file where: Ship-to Country = #',
             len(IB[IB['Ship-to Country']=='#']),
             np.NaN,
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    ###Ship to country not assigned
    serie = ['Number of printers with country specified',
             'Count printers from IB file where: Ship-to Country != #',
             len(IB[IB['Ship-to Country']!='#']),
             np.NaN,
             np.NaN]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    
    
    print('-------------------------------------')
    print('Last steps')
    ######Number of cases where a part was changed with a quantity >1 while
    ######this part should be replacedonly once
    count_cases_replacement_qty_greater_than_one_same_part_from_notif = 0
    list_all_parts_changed = [x for x in notif['Parts changed'].values if str(x) != 'nan']
    for parts_changed in list_all_parts_changed:
        if len([parts_changed[k] for k in parts_changed if k in list_references_unique_replacement and parts_changed[k]>1]):
            count_cases_replacement_qty_greater_than_one_same_part_from_notif += len([parts_changed[k] for k in parts_changed if k in list_references_unique_replacement and parts_changed[k]>1])
            
    count_cases_replacement_qty_greater_than_one_same_part_from_notif_filtered = 0
    list_all_parts_changed = [x for x in notif_filtered['Parts changed'].values if str(x) != 'nan']
    for parts_changed in list_all_parts_changed:
        if len([parts_changed[k] for k in parts_changed if k in list_references_unique_replacement and parts_changed[k]>1]):
            count_cases_replacement_qty_greater_than_one_same_part_from_notif_filtered += len([parts_changed[k] for k in parts_changed if k in list_references_unique_replacement and parts_changed[k]>1])

    count_cases_replacement_qty_greater_than_one_same_part_from_notif_filtered_under_scope = 0
    list_all_parts_changed = [x for x in notif_filtered_and_under_scope['Parts changed'].values if str(x) != 'nan']
    for parts_changed in list_all_parts_changed:
        if len([parts_changed[k] for k in parts_changed if k in list_references_unique_replacement and parts_changed[k]>1]):
            count_cases_replacement_qty_greater_than_one_same_part_from_notif_filtered_under_scope += len([parts_changed[k] for k in parts_changed if k in list_references_unique_replacement and parts_changed[k]>1])

    serie = ["Number Of cases where a part was chnaged with a quantity > 1 while it shouldn't",
             'For each notif, count the the quantitry changed for each componenent and then count the number of cases where a component was changed with a quantity > 1 while it should be replaced with a quantity equal to 1',
             count_cases_replacement_qty_greater_than_one_same_part_from_notif,
             count_cases_replacement_qty_greater_than_one_same_part_from_notif_filtered,
             count_cases_replacement_qty_greater_than_one_same_part_from_notif_filtered_under_scope]
    metrics = metrics.append(pd.Series(serie, index=metrics.columns), ignore_index=True)
    
    
    print('-------------------------------------')
    print('Saving file')
    
    
    with pd.ExcelWriter(excel_output_path, mode='w', engine='xlsxwriter') as writer: 
        sheet_name='metrics'
        metrics.to_excel(writer, sheet_name=sheet_name, index=False)
        format_tbl(writer, sheet_name, metrics)
    
    
    print('-------------------------------------')
    print('End of calculation')
    
    return(notif, notif_filtered, notif_filtered_and_under_scope, 
           IB, IB_filtered, IB_filtered_and_under_scope)



if __name__ == '__main__':
        
    ##################               9450C              #########################
    notif_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9450C\9450C Ec Sc Notification 2015 To 2021 Extraction.xlsx'
    IB_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9450C\9450C Ec Sc IB February 2021 Extraction.xlsx'
    contracts_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9450C\Contract created between 01-01-200 and 27-07-2021- From Renewal Query in BW.xlsx'
    sap_mvt_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9450C\SAP Parts Mvt.xlsx'
    ##################               9410               #########################
    notif_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9410\Notifications.xlsx'
    IB_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9410\Historic IB.xlsx'
    contracts_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9410\Contract Base.xlsx'
    sap_mvt_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9410\Part Movement.xlsx'
    ##################         9018-28-29-9330         ##########################
    notif_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9018-28-29-9330\Notifications.xlsx'
    IB_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9018-28-29-9330\IB.xlsx'
    contracts_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9018-28-29-9330\Contract Base.xlsx'
    sap_mvt_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9018-28-29-9330\SAP MVT.xlsx'
    ##################               9450               #########################
    notif_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9450\Notifications.xlsx'
    IB_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9450\Historic IB.xlsx'
    contracts_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9450\Contract Base.xlsx'
    sap_mvt_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9450\Part Movement.xlsx'
    parts_data_path = r'C:\Users\mjmel\Desktop\Internship\Data new batch\9450 C Parts Data.xlsx'
    
    
    
    
    
    parts_data = pd.read_excel(parts_data_path, header=1)
    parts_data['Part N°'] = parts_data['Part N°'].astype('str')
    
    list_components_unique_replacement = ['STRENGTHEN PRESSURE PUMP KIT', 'kit NEP + techno board',
                                          'Print module-9450C', 'MODULATION BODY', 'Deflection plates kit',
                                          'Gutter block kit', 'EHV Cover', 'Board - Head', 'Cover - Head - Stainless steel']
    list_references_unique_replacement = parts_data[parts_data['Group parts'].isin(list_components_unique_replacement)]['Part N°'].unique()

    
    
    printer_type = '9450'
    excel_output_path = r'C:\Users\mjmel\Desktop\Internship\Metrics\metrics_{}_{}.xlsx'.format(printer_type, str(date.today()))

    notif, notif_filtered, notif_filtered_and_under_scope, IB, IB_filtered, IB_filtered_and_under_scope = calculate_metrics(notif_path, IB_path, contracts_path,
                      sap_mvt_path, printer_type, excel_output_path, list_components_unique_replacement)
