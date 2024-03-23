A Full end to end solution using Fabric Lakehouse

0- Create a Fabric Workspace

1- Create a lakehouse

2-Download the notebook from Github and import it to Fabric Workspace

3-open a notebook, attached it to the Lakehouse

4-Run the notebook 1, new data arrive at 5 am Brisbane time, AEMO keep an archive for 60 days ( add a schedule to keep it updated)

5-Run notebooks 2, it is one off operation ( calendar and states tables)

6-import the pbix to get PowerBI report working in import mode, change the source connection to Point to the new SQL endpoint


<img width="733" alt="image" src="https://github.com/djouallah/aemo_fabric/assets/12554469/62a5ac05-34b7-4ad8-af74-6d8d92a211a3">


## Optional

7- run notebook 3 to create a new Direct Lake semantic model that uses the existing import mode

8- run notebook 4 to have the data updated every 5 minutes ( add a schedule to keep it updated )

