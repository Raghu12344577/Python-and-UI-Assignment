# -*- coding: utf-8 -*-
"""
Created on Thu Dec 23 17:48:46 2021

@author: s.raghunathan
"""

import pandas as pd
import sqlite3 as db

def InsertDataSQLite(databasePath,table1,table2,table3,demandOrderFile,supplyOrderFile,sourcingRuleFile):
    conn = db.connect(databasePath,);
    
    df_demand_order = pd.read_csv(demandOrderFile)
    df_demand_order.to_sql(name=table1, con=conn,index=False,if_exists="replace")
    
    df_supply = pd.read_csv(supplyOrderFile)
    df_supply.to_sql(name=table2, con=conn,index=False,if_exists="replace")
    
    df_sourcing_rule = pd.read_csv(sourcingRuleFile)
    df_sourcing_rule.to_sql(name=table3, con=conn,index=False,if_exists="replace")
    
    conn.commit()
    
dbpath = "D:\s.raghunathan\products_order.db"
table1name = 'demand_order'
table2name = 'supply'
table3name = 'sourcing_rule'
demandOrderFile = "D:\s.raghunathan\data sample\data sample\demand_order.csv"
supplyOrderFile = "D:\s.raghunathan\data sample\data sample\supply.csv"
sourcingRuleFile = "D:\s.raghunathan\data sample\data sample\sourcing_rule.csv"
InsertDataSQLite(dbpath,table1name,table2name,table3name,demandOrderFile,supplyOrderFile,sourcingRuleFile)