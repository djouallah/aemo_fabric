## A Full end to end solution using Fabric Lakehouse

AEMO manage electricity and gas systems and markets across Australia,  they provide extensive data at a very granular level and updated every 5 minutes, see example here
https://nemweb.com.au/Reports/CURRENT/

<img width="503" alt="image" src="https://github.com/djouallah/aemo_fabric/assets/12554469/464009ce-f424-40c5-b35e-8a93cd80f574">


## Architecture

<img width="742" alt="image" src="https://github.com/djouallah/aemo_fabric/assets/12554469/96145683-7cea-4b4f-80fb-089ae63731cf">



## Howto

0- Create a Fabric Workspace

1- Create a lakehouse

2-Download the notebook from Github and import it to Fabric Workspace

3-Open Notebook 1, attached it to the Lakehouse then run it, new data arrive at 5 am Brisbane time, AEMO keep an archive for 60 days ( add a schedule to keep it updated)

4-Open Notebook 2, attached it to the Lakehouse then run it, it is one off operation ( calendar and states tables)

5-import the pbix to get PowerBI report working in import mode, change the source connection to Point to the new SQL endpoint


<img width="881" alt="image" src="https://github.com/djouallah/aemo_fabric/assets/12554469/90dfe7c9-0258-4976-8c45-3b7c6322882d">



## Optional

6- run notebook 3 to create a new Direct Lake semantic model that uses the existing import mode

7- run notebook 4 to have the data updated every 5 minutes ( add a schedule to keep it updated )

## Lessons learnt

-	Use data pipeline to schedule job, to control concurrency and timeout.

  
-	Develop using starter pool, but for production use a single node to reduce capacity usage.

-	 Direct don't like too many small files, optimize to get good performance

-	 vacuum to remove old snapshots to reduce data storage. 


