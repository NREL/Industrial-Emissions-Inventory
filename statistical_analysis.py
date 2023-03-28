"""
This code performs statistical analysis for generated datasets
"""


import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.ensemble import IsolationForest
import sys
import numpy as np


import warnings
warnings.simplefilter(action='ignore')
from datetime import datetime
dt = datetime.today().strftime('%Y-%m-%d')



result_folder = dt
out_path1 = "../"+dt


sectors = ['hydrogen','cement','steel','ammonia','refining','natural_gas_processing','ethanol','pulp']

for sec in sectors:
    
        #Reading the facility level inventory file for emissions for each sector
        fac_lev_df = pd.read_csv(out_path1+"/"+sec+"/"+sec+"_inventory_facility_level.csv")
        print("Total Number of Facilities - "+str(len(list(pd.unique(fac_lev_df['FRS_ID'])))) +" in sector " +sec)
        ghgrp = fac_lev_df[fac_lev_df['Source'] == "GHGRP"]
        nei = fac_lev_df[fac_lev_df['Source'] == "NEI"]
        tri = fac_lev_df[fac_lev_df['Source'] == "TRI"]
        
        #Printing number of facilities from each database
        print("GHGRP facilities - "+str(len(list(pd.unique(ghgrp['FRS_ID'])))) +" in sector " +sec)
        print("NEI facilities - "+str(len(list(pd.unique(nei['FRS_ID'])))) +" in sector " +sec)
        print("TRI facilities - "+str(len(list(pd.unique(tri['FRS_ID'])))) +" in sector " +sec)
        
        """
        Outlier analysis can only be performed for certain emission attributes like CO2 concentration. We focus on calculated co2 concentration
        for the statistical analysis
        """
        fac_lev_df = pd.read_csv(out_path1+"/"+sec+"/"+sec+"_facility_level_with_concentration_via_co2_reference.csv")
        co2 = fac_lev_df[fac_lev_df['FlowName'] == 'Carbon Dioxide']
        fig = plt.figure()
        #Historgram plot for distribution of total CO2 amounts in the inventory
        sns.histplot(x=co2['FlowAmount'],bins = 50)
        plt.title('Carbon dioxide Kgs')
        plt.xticks([])
        fig.savefig(out_path1+"/"+sec+"/"+sec+'_co2_total_flow_histogram.pdf',bbox_inches='tight')

        
        def mean_median(df,comment):
            print(comment)
            print("Mean " + comment + " " + str(np.mean(df['Concentration'])))
            print("Median " + comment + " " + str(np.median(df['Concentration'])))
            print("")
            print("")




        #Redo plots
        
        def plotting(fac_lev_df,figname):
            """
            Parameters
            ----------
            fac_lev_df : pd dataframe
               pandas dataframe for the inventory
                
            figname : str
               name of the plot figure being generated

            Returns
            -------
            None.

            """
            
            a = 2
            b = 2
            fig, axs = plt.subplots(a, b,figsize=(35, 20))
            
            a = 0
            b = 0
            emissions = ['Carbon Dioxide', 'Methane', 'Nitrous Oxide','PM10-PM2.5']
            for em in emissions:
                
                if b > 1:
                    a = a + 1
                    b = 0
                
                em_df = fac_lev_df[fac_lev_df['FlowName'] == em]
                axs[a,b] = sns.scatterplot(ax=axs[a, b], x=em_df['FRS_ID'], y=em_df['Concentration'])
                axs[a,b].set_title(em, fontsize=30)
                axs[a,b].tick_params(axis='y', labelsize=30)
                axs[a,b].set_ylabel(ylabel = 'Concentration kg/kg',fontsize=30)
                axs[a,b].tick_params(axis='x', labelsize=30)
                axs[a,b].set_xticklabels(labels = [])
                b = b +1
            
            fig.subplots_adjust(bottom=0.2)
            fig.savefig(out_path1+"/"+sec+"/"+figname+'.pdf',bbox_inches='tight')
        
        
        plotting(fac_lev_df,sec+"_plot")
        #Searching for outliers
        frs_id_outliers = []
        
        def outlier_isolater(em_df):
            """
            Parameters
            ----------
            em_df : pandas dataframe
                emissions dataframe

            Returns
            -------
            frs_id_outlier_list : list of strings
                list of strings containing the list of FRS IDS which are deemed to be outliers

            """
            em_df.loc[:,'FRS_ID'] = em_df.loc[:,'FRS_ID'].astype('str')
            
            #Finding outliers using Random Forest Model
            clf = IsolationForest(max_samples= "auto",random_state = 1, contamination= 0.01)
            try:
                clf.fit(em_df[['Concentration']])
            except:
                em_df = em_df.dropna(subset = ['Concentration'])
                clf.fit(em_df[['Concentration']])
            
            preds = clf.predict(em_df[['Concentration']])
            em_df['outliers'] = preds
            frs_id_with_outlier = em_df[em_df['outliers'] == -1]
            frs_id_outlier_list = list(pd.unique(frs_id_with_outlier['FRS_ID']))
            return frs_id_outlier_list
        
        
        
        
        #Searching for Outliers for different pollutants separately. 
        emissions = ['Carbon Dioxide', 'Methane', 'Nitrous Oxide','PM10-PM2.5']
        for em in emissions:
            em_df = fac_lev_df[fac_lev_df['FlowName'] == em]
            mean_median(em_df,em)
            frs_id_outliers.append(outlier_isolater(em_df))
        # Compiling the completel outlier list for different pollutants and creating an universal outlier list   
        final_outlier_list = []
        for i in frs_id_outliers:
            for j in i:
                final_outlier_list.append(j)
                
        
        #Removing outliers
        fac_lev_df.loc[:,'FRS_ID'] = fac_lev_df.loc[:,'FRS_ID'].astype('str')
        fac_lev_df_cleaned = fac_lev_df[~fac_lev_df['FRS_ID'].isin(final_outlier_list)]
        fac_lev_df_outliers = fac_lev_df[fac_lev_df['FRS_ID'].isin(final_outlier_list)]
        
        fac_lev_df_cleaned.to_csv(out_path1+"/"+sec+"/"+sec+"cleaned_after_outlier_removal.csv")   
        fac_lev_df_outliers.to_csv(out_path1+"/"+sec+"/"+sec+"_outliers.csv")   
        plotting(fac_lev_df_cleaned,sec+"_cleaned_plot")


        emissions = ['Carbon Dioxide', 'Methane', 'Nitrous Oxide','PM10-PM2.5']
        for em in emissions:
            em_df = fac_lev_df_cleaned[fac_lev_df_cleaned['FlowName'] == em]
            mean_median(em_df,em)
