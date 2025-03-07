"""
This code performs statistical analysis for generated datasets
"""


import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.ensemble import IsolationForest
import sys
import numpy as np
from decimal import Decimal


import warnings
warnings.simplefilter(action='ignore')
from datetime import datetime
dt = datetime.today().strftime('%Y-%m-%d')



"Using current date as results folder name"
result_folder = dt
out_path1 = "../"+dt


def stat_analysis(sec):
    """
    This function performs statistical analysis for every industrial sector. In a scatter plot of concentrations of facility emissions across a sector, outliers were observed. To identify and eliminate statistical outliers, we performed an outlier detection analysis on the pollutant concentrations of four major pollutants - CO2, Methane, Nitrous Oxide and PM10-PM2.5 The Isolation Forest model was used for outlier detection (Liu, Ting, and Zhou 2008). 
    A list of facilities is obtained from separate outlier analysis for each of these emission concentrations. The lists for these four emissions are combined to create a complete list of outliers. All these outlier facilities are removed to obtain the clean inventory. The output file is sector_cleaned_after_outlier_removal.csv. The outlier facilities removed from the inventory for every sector is provided in the corresponding sector_outliers.csv file. 

    Parameters 
    ----------
    sec: str
     Industrial sector name. example: cement, hydrogen

    

    Output
    ------
    The output file is sector_cleaned_after_outlier_removal.csv. The outlier facilities removed from the inventory for every sector is provided in the corresponding sector_outliers.csv file. 
    

    Returns
    -------
    None

    """
    
    emissions_for_plotting = ['Volatile Organic Compounds', 'Sulfur Dioxide', 'Nitrogen Oxides','PM10-PM2.5']
    emissions_for_outlier_removal= ['Volatile Organic Compounds', 'Sulfur Dioxide', 'Nitrogen Oxides','PM10-PM2.5','Methane','Carbon Dioxide','Nitrous Oxide']
 
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
    sns.histplot(data=co2['FlowAmount'],bins = 50)
    plt.title('Carbon dioxide Kgs')
    #plt.xticks([])
    plt.xlabel('Flow Amount kgs',fontsize = 10)
    plt.ylabel('Facility Count',fontsize = 10)
    fig.savefig(out_path1+"/"+sec+"/"+sec+'_co2_total_flow_histogram.pdf',bbox_inches='tight')


    co2 = fac_lev_df[fac_lev_df['FlowName'] == 'Sulfur Dioxide']
    fig = plt.figure()
    #Historgram plot for distribution of total CO2 amounts in the inventory
    sns.histplot(x=co2['FlowAmount'],bins = 50)
    plt.title('Sulfur Dioxide Kgs')
    #plt.xticks([])
    plt.xlabel('Flow Amount kgs',fontsize = 10)
    plt.ylabel('Facility Count',fontsize = 10)
    fig.savefig(out_path1+"/"+sec+"/"+sec+'_so2_total_flow_histogram.pdf',bbox_inches='tight')

    co2 = fac_lev_df[fac_lev_df['FlowName'] == 'Nitrogen Oxides']
    fig = plt.figure()
    #Historgram plot for distribution of total CO2 amounts in the inventory
    sns.histplot(x=co2['FlowAmount'],bins = 50)
    plt.title('Nitrogen Oxides Kgs')
    #plt.xticks([])
    plt.xlabel('Flow Amount kgs',fontsize = 10)
    plt.ylabel('Facility Count',fontsize = 10)
    fig.savefig(out_path1+"/"+sec+"/"+sec+'_nox_total_flow_histogram.pdf',bbox_inches='tight')


    
    def mean_median(df,comment,sec):
        
        mean_list = []
        median_list = []
        comment_list = []
        sector_list = []
        minimum_list = []
        maximum_list = []
        print(comment)

        df_r = pd.DataFrame()

        print("Mean " + comment + " " + str(np.mean(df['Concentration'])))
        print("Median " + comment + " " + str(np.median(df['Concentration'])))
        print("Emissions " + comment + " " + str(comment))

        mean_list.append(f"{Decimal(np.mean(df['Concentration'])):.4E}")
        median_list.append(f"{Decimal(np.median(df['Concentration'])):.4E}")
        comment_list.append(str(comment))
        sector_list.append(sec)
        minimum_list.append(f"{Decimal(min(df['Concentration'])):.4E}")
        maximum_list.append(f"{Decimal(max(df['Concentration'])):.4E}")

        df_r['mean'] = mean_list
        df_r['median'] = median_list
        df_r['emissions'] = comment_list
        df_r['sector'] = sector_list
        df_r['minimum'] = minimum_list
        df_r['maximum'] = maximum_list

        return df_r

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
        
        for em in emissions_for_plotting:
            
            if b > 1:
                a = a + 1
                b = 0
            
            em_df = fac_lev_df[fac_lev_df['FlowName'] == em]
            axs[a,b] = sns.scatterplot(ax=axs[a, b], x=em_df['FRS_ID'], y=em_df['Concentration'])
            axs[a,b].set_title(em, fontsize=30)
            axs[a,b].tick_params(axis='y', labelsize=30)
            axs[a,b].set_ylabel(ylabel = 'Concentration kg/kg',fontsize=30)
            axs[a,b].set_xlabel(xlabel = 'FRS_ID',fontsize=20)
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

    for em in emissions_for_outlier_removal:
        em_df = fac_lev_df[fac_lev_df['FlowName'] == em]
        mean_median(em_df,em,sec)
        frs_id_outliers.append(outlier_isolater(em_df))

    # Compiling the complete outlier list for different pollutants and creating an universal outlier list   
    final_outlier_list = []
    for i in frs_id_outliers:
        for j in i:
            final_outlier_list.append(j)
            
    
    #Removing outliers
    fac_lev_df.loc[:,'FRS_ID'] = fac_lev_df.loc[:,'FRS_ID'].astype('str')
    fac_lev_df_cleaned = fac_lev_df[~fac_lev_df['FRS_ID'].isin(final_outlier_list)]
    fac_lev_df_outliers = fac_lev_df[fac_lev_df['FRS_ID'].isin(final_outlier_list)]
    
    fac_lev_df_cleaned.to_csv(out_path1+"/"+sec+"/"+sec+"_cleaned_after_outlier_removal.csv")   
    fac_lev_df_outliers.to_csv(out_path1+"/"+sec+"/"+sec+"_outliers.csv")   
    plotting(fac_lev_df_cleaned,sec+"_cleaned_plot")

    df_statistics = pd.DataFrame()

    for em in emissions_for_outlier_removal:
        em_df = fac_lev_df_cleaned[fac_lev_df_cleaned['FlowName'] == em]
        df_statistics = pd.concat([df_statistics,mean_median(em_df,em,'w/o outliers')])
        em_df_c = fac_lev_df[fac_lev_df['FlowName'] == em]
        df_statistics = pd.concat([df_statistics,mean_median(em_df_c,em,'w. outliers')])


    df_statistics.to_csv(out_path1+"/"+sec+"/"+sec+"statistics_summary.csv")
    df_with_outliers = pd.read_csv(out_path1+"/"+sec+"/"+sec+"_facility_level_with_concentration_via_co2_reference.csv")
    df_without_outliers = pd.read_csv(out_path1+"/"+sec+"/"+sec+"_cleaned_after_outlier_removal.csv") 
    df_with_outliers['outliers'] = 'before outlier removal'
    df_without_outliers['outliers'] = 'after outlier removal'
    total_df = pd.concat([df_with_outliers,df_without_outliers])
    total_df = total_df.reset_index()



    def plotting2(fac_lev_df):
        """
        Parameters
        ----------
        fac_lev_df : pd dataframe
           pandas dataframe for the inventory
            

        Returns
        -------
        None.

        """
        
        a = 2
        b = 2
        fig, axs = plt.subplots(a, b,figsize=(35, 20))
        a = 0
        b = 0
        
        for em in emissions_for_plotting:
            
            if b > 1:
                a = a + 1
                b = 0
            
            em_df = fac_lev_df[fac_lev_df['FlowName'] == em]
            axs[a,b] = sns.histplot(ax=axs[a, b],x=em_df['Concentration'],bins = 50,hue = em_df['outliers'])
            axs[a,b].set_title(em, fontsize=15)
            axs[a,b].tick_params(axis='y', labelsize=15)
            axs[a,b].set_xlabel(xlabel = 'Concentration kg/kg',fontsize=15)
            axs[a,b].tick_params(axis='x', labelsize=15)

            b = b + 1
        
        fig.subplots_adjust(bottom=0.2)
        fig.savefig(out_path1+"/"+sec+"/"+sec+'_comparison_histogram.pdf',bbox_inches='tight')


    plotting2(total_df)


    def plotting3(fac_lev_df):
        """
        Parameters
        ----------
        fac_lev_df : pd dataframe
           pandas dataframe for the inventory
            

        Returns
        -------
        None.

        """
        
        a = 2
        b = 2
        fig, axs = plt.subplots(a, b,figsize=(35, 20))
        
        a = 0
        b = 0
        
        for em in emissions_for_plotting:
            
            if b > 1:
                a = a + 1
                b = 0
            
            em_df = fac_lev_df[fac_lev_df['FlowName'] == em]
            axs[a,b] = sns.boxplot(ax=axs[a, b],x=em_df['Concentration'], y = em_df['outliers'], hue = em_df['outliers'])
            axs[a,b].set_title(em, fontsize=10)
            axs[a,b].tick_params(axis='y', labelsize=10)
            axs[a,b].set_xlabel(xlabel = 'Concentration kg/kg',fontsize=10)
            axs[a,b].tick_params(axis='x', labelsize=10)
            axs[a,b].set_yticklabels(labels = [])
            b = b +1
        
        fig.subplots_adjust(bottom=0.2)
        fig.savefig(out_path1+"/"+sec+"/"+sec+'_comparison_box.pdf',bbox_inches='tight') 


    plotting3(total_df)        





