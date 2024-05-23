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


sectors = ['cement']

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
        emissions = ['Carbon Dioxide', 'Nitrogen Oxides','Sulfur Dioxide', 'Nitrous Oxide','PM10-PM2.5','Volatile Organic Compounds']
        for em in emissions:
            print(em)
            co2 = fac_lev_df[fac_lev_df['FlowName'] == em]
            fig = plt.figure()
            #Historgram plot for distribution of total CO2 amounts in the inventory
            sns.histplot(x=co2['FlowAmount'],bins = 50)
            plt.title(em+' Kgs')
            div = 1
            print(str(sum(co2['FlowAmount'])) + 'kg')
            print(str(sum(co2['FlowAmount'])/div*100) + '%')
            print(str(sum(co2['FlowAmount'])/1000000000) + 'million metric tonnes')
            fig.savefig(out_path1+"/"+sec+"/"+sec+em+'_total_flow_histogram.pdf',bbox_inches='tight')


            fig = plt.figure()
            #Historgram plot for distribution of total CO2 amounts in the inventory
            sns.histplot(x=co2['Concentration'],bins = 50)
            plt.title(em+' kg/kg')
            div = 1
            print(str(sum(co2['Concentration'])) + 'kg/kg')
            fig.savefig(out_path1+"/"+sec+"/"+sec+em+'_total_concentration_histogram.pdf',bbox_inches='tight')
            print('no of facilities')
            print(len(pd.unique(co2['FRS_ID'])))


        stack_pm_df = pd.read_csv(out_path1+"/"+sec+"/"+sec+"_nei_flat_fac_level_info_for_stack_parameters.csv")
        print(stack_pm_df.columns)
        stack_pm_df_dropped = stack_pm_df.dropna(subset = stack_pm_df.columns, how = 'all')
        print(len(pd.unique(stack_pm_df_dropped['FACILITY_ID'])))
        fig = plt.figure()
        #Historgram plot for distribution of total CO2 amounts in the inventory
        sns.histplot(x=stack_pm_df_dropped['STKTEMP'],bins = 50)
        plt.title('stacktemp')
        fig.savefig(out_path1+"/"+sec+"/"+sec+'_stk_temp_histogram.pdf',bbox_inches='tight')
        print(len(stack_pm_df_dropped))
        count = stack_pm_df_dropped[(stack_pm_df_dropped['STKTEMP'] > 60)& (stack_pm_df_dropped['STKTEMP'] < 200)]
        print(len(count))


        fig = plt.figure()
        print('HEIGHT')
        #Historgram plot for distribution of total CO2 amounts in the inventory
        sns.histplot(x=stack_pm_df_dropped['STKHGT'],bins = 50)
        plt.title('stackheight')
        fig.savefig(out_path1+"/"+sec+"/"+sec+'_stk_height_histogram.pdf',bbox_inches='tight')
        print(len(stack_pm_df_dropped))
        count = stack_pm_df_dropped[(stack_pm_df_dropped['STKHGT'] > 1)& (stack_pm_df_dropped['STKHGT'] < 90)]
        print(len(count))

        def mean_median(df,comment):
            print(comment)
            print("Mean " + comment + " " + str(np.mean(df['Concentration'])*1000))
            print("Median " + comment + " " + str(np.median(df['Concentration'])*1000))
            print("25th percentle " + comment + " " + str(np.percentile(df['Concentration'],25)*1000))
            print("75th percentile" + comment + " " + str(np.percentile(df['Concentration'],75)*1000))
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
            

            
            
            a = 0
            b = 0
            emissions = ['Carbon Dioxide', 'Nitrogen Oxides','Sulfur Dioxide', 'Nitrous Oxide','PM10-PM2.5','Volatile Organic Compounds']
            for em in emissions:
                
                fig = plt.figure(figsize=(12, 10))
                em_df = fac_lev_df[fac_lev_df['FlowName'] == em]
                ax=sns.scatterplot(x=em_df['FRS_ID'], y=em_df['Concentration'] )
                sns.set_style("white")
                sns.set_context("poster")
                ax.set_title(em, fontsize=15)
                ax.tick_params(axis='y', labelsize=20)
                ax.set_ylabel(ylabel = 'Concentration kg/kg',fontsize=15)
                ax.tick_params(axis='x', labelsize=15)
                ax.set_xticklabels(labels = [])
                fig.savefig(out_path1+"/"+sec+"/"+figname+em+'.pdf',bbox_inches='tight') 
            
        
        plotting(fac_lev_df,sec+"scatter_plot")
        #Searching for outliers
        frs_id_outliers = []

        def scatterplotting(fac_lev_df,figname):
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
            

            
            
            a = 0
            b = 0
            emissions = ['Carbon Dioxide', 'Nitrogen Oxides','Sulfur Dioxide', 'Nitrous Oxide','PM10-PM2.5','Volatile Organic Compounds']
            for em in emissions:
                
                fig = plt.figure(figsize=(12, 10))
                em_df = fac_lev_df[fac_lev_df['FlowName'] == em]
                ax=sns.scatterplot(x=em_df['FRS_ID'], y=em_df['Concentration'], hue = em_df['outlier_analysis'] )
                sns.set_style("white")
                sns.set_context("poster")
                ax.set_title(em, fontsize=15)
                ax.tick_params(axis='y', labelsize=20)
                ax.set_ylabel(ylabel = 'Concentration kg/kg',fontsize=15)
                ax.tick_params(axis='x', labelsize=15)
                ax.set_xticklabels(labels = [])
                fig.savefig(out_path1+"/"+sec+"/"+figname+em+'.pdf',bbox_inches='tight') 
            
        
        plotting(fac_lev_df,sec+"scatter_plot")
        #Searching for outliers
        frs_id_outliers = []

        def boxplotting(fac_lev_df,figname):
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
            

            
            
            a = 0
            b = 0
            emissions = ['Carbon Dioxide', 'Nitrogen Oxides','Sulfur Dioxide', 'Nitrous Oxide','PM10-PM2.5','Volatile Organic Compounds']
            for em in emissions:
                
                fig = plt.figure(figsize=(30, 10))
                sns.set_style("white")
                sns.set_context("poster")
                em_df = fac_lev_df[fac_lev_df['FlowName'] == em]
                ax=sns.boxplot(y=em_df['Concentration'],x = em_df['outlier_analysis'])
                ax.set_title(em, fontsize=15)
                ax.tick_params(axis='y', labelsize=20)
            
                fig.savefig(out_path1+"/"+sec+"/"+figname+em+'boxplot.pdf',bbox_inches='tight')     
            




        
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
        emissions = ['Carbon Dioxide', 'Nitrogen Oxides','Sulfur Dioxide', 'Nitrous Oxide','PM10-PM2.5','Volatile Organic Compounds']
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
        plotting(fac_lev_df_cleaned,sec+"scatter_cleaned_plot")


        emissions = ['Carbon Dioxide', 'Nitrogen Oxides','Sulfur Dioxide', 'Nitrous Oxide','PM10-PM2.5','Volatile Organic Compounds']
        for em in emissions:
            em_df = fac_lev_df_cleaned[fac_lev_df_cleaned['FlowName'] == em]
            mean_median(em_df,em)

        fac_lev_df_cleaned['outlier_analysis'] = 'w/o outliers'
        fac_lev_df['outlier_analysis'] = 'w outliers'

        total_df = pd.concat([fac_lev_df,fac_lev_df_cleaned])
        boxplotting(total_df,sec+"_box_plot")
        scatterplotting(total_df,sec+"_scatter_plot")









        def plotting2(fac_lev_df,figname):
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
            emissions = ['Carbon Dioxide', 'Nitrogen Oxides','Sulfur Dioxide', 'Nitrous Oxide','PM10-PM2.5','Volatile Organic Compounds']
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

