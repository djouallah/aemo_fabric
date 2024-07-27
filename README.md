## A Full end to end solution using Fabric Lakehouse

AEMO manage electricity and gas systems and markets across Australia,  they provide extensive data at a very granular level and updated every 5 minutes, see example here
https://nemweb.com.au/Reports/CURRENT/

<img width="503" alt="image" src="https://github.com/djouallah/aemo_fabric/assets/12554469/464009ce-f424-40c5-b35e-8a93cd80f574">


## Architecture

<img width="742" alt="image" src="https://github.com/djouallah/aemo_fabric/assets/12554469/96145683-7cea-4b4f-80fb-089ae63731cf">



## Howto

1- Create a Fabric Workspace

2- Fork this repo, and connect Fabric to it : https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-integration-process?tabs=azure-devops#connect-and-sync

3-Open Notebook Initial Setup, attach the Lakehouse "storage" then run it,it will automatically:

  Attach the lakehouse to the two notebooks, and load data for 1 day, you can change the parameter to 60 days notebookutils.notebook.run("Process_Data_Every_24_Hours", 2000,{"Nbr_Files_to_Download": 1 })
  
  rebind the semantic model to the new Lakehouse
  
  rebind the report to the new semantic model

<img width="1000" alt="image" src="https://github.com/user-attachments/assets/9d3533b7-4bbc-42dd-b8e7-3dc11b19e571">

and here is the PowerBI report



<img width="1106" alt="image" src="https://github.com/djouallah/aemo_fabric/assets/12554469/0c802002-9478-49bc-b9b4-0bb37c8ce93c">




## Optional


3- turn on scheduler for 5 minutes and daily, notice daily files get updated at 6 AM Brisbane time.

## Lessons learnt

-	Use data pipeline to schedule job, to control concurrency and timeout.

-	Develop using starter pool, but for production use a single node to reduce capacity usage.

-	Direct Lake don't like too many small files, run SQL optimize to get good performance

-	Vacuum to remove old snapshots to reduce data storage. 


