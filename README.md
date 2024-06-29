# ETL
Documentation:

1. All the required packages are included in requirements.txt
2. Script folder contains table definations and DB statement
3. Data folder has Input data that is used in the Workflow
4. Data folder also had Output folder that contains screenshots of plots
5. Metaflow folder has 3 pipeline which can be step wise used they are quite explainetory on there own and perform very well
     1. Clean raw data (1_clean_rawdata) will fetch data and load it in DB
     2. our second file (2_load_clean_data) will fetch our DB data and perform some IMP normalization and calculate metrics
     3. Our plots are majorly residing in 3 Workflow (3_fetch_data_and_process) This will process some data and make PDF output of the plots and store the in /Data/Output
6. Our ETL/connection file is Used to handle all the DB related operation
7. Run Instructions
     We can simply install libs and start executing workflow from metaflow folder
     The number on file names are best execution order
    
