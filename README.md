# pypi_graph

Building PyPi Graph Data Pipeline

# Introduction

This project is a data pipeline loading 
pypi rawdata built by my `pypi_rawdata` etl pipeline and transform the data into a graph representation. 

# The graph representation

... 


# Development Plan 

- [X] Build up 2 layers of tabular data to graph data transformation
    - [X] Layer1: extract node and link tables from each rawdata table.
    - [X] Layer2: group node/link tables that map to the same kind of nodes or links. 
- [X] Milestone: Make sure the Data Pipeline classes can be initialized in a main.py file. 
    NOTE: using LocalBackend of batch_framework
    not using DropboxBackend yet.
- [ ] Migrade pypi rawdata from Dropbox to local
- [ ] Run local table to graph transformation
- [ ] Migrade all subgraph, graph data to Dropbox
- [ ] Make sure the data pipeline works with Dropbox as storage 
- [ ] Build up github action pipeline
    - [ ] Move neccessary files related to building of workflow ymls
    - [ ] Generate workflow ymls
    - [ ] Push the workflow to github action and make sure it runs succesfully

