## A Full end to end solution using Fabric Lakehouse

AEMO manage electricity and gas systems and markets across Australia,  they provide extensive data at a very granular level and updated every 5 minutes, see example here
https://nemweb.com.au/Reports/CURRENT/

<img width="503" alt="image" src="https://github.com/djouallah/aemo_fabric/assets/12554469/464009ce-f424-40c5-b35e-8a93cd80f574">


## Architecture

<img width="742" alt="image" src="https://github.com/djouallah/aemo_fabric/assets/12554469/96145683-7cea-4b4f-80fb-089ae63731cf">



## Howto

0- Create a Fabric Workspace

1- connect to the github report from Fabric

2-Open Notebook 1, attached it to the Lakehouse then run it, new data arrive at 5 am Brisbane time, AEMO keep an archive for 60 days ( add a schedule to keep it updated)

3-Open Notebook 2, run it, it will rebind the semantic model and the report to the new create Lakehouse


<img width="1106" alt="image" src="https://github.com/djouallah/aemo_fabric/assets/12554469/0c802002-9478-49bc-b9b4-0bb37c8ce93c">




## Optional


3- Open notebook 3 and attach a lakehouse, turn on the data pipeline scheduler if you want 5 minutes data.

## Lessons learnt

-	Use data pipeline to schedule job, to control concurrency and timeout.

-	Develop using starter pool, but for production use a single node to reduce capacity usage.

-	 Direct don't like too many small files, optimize to get good performance

-	 vacuum to remove old snapshots to reduce data storage. 


