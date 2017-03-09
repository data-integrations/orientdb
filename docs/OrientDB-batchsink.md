# OrientDB Batch Sink

This plugin writes records into [OrientDB](http://orientdb.com/orientdb/). It uses the Graph API

Writes data to OrientDB database. It uses the [Graph API](http://orientdb.com/docs/2.2/Graph-Database-Tinkerpop.html) of OrientDB to create vertices and edges between the vertices. 

> NOTE : This is a very early version and doesn't have all the functionlities exposed from OrientDB. 


## Plugin Configuration

| Configuration | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Connection String** | **Y** | N/A | This configuration specifies the orient db server endpoint to ingest into. |
| **Row Key** | **Y** | N/A | Specifies how unique key needs to be generated. This can be an expression. |
| **User Name** | **Y** | N/A | If OrientDB is configured in secured mode with authentication turned on, then specify the user name to be used when connecting to OrientDB |
| **Password** | **Y** | N/A | Specifies the password for the realm specified above. |
| **Vertex Column Name** | **Y** | N/A | Specifies the name of the input column name that should be used considered as a vertices for the graph |
| **Edge Column Name** | **Y** | N/A | Specifies the name of the input column name that should be used for defining the edge between the vertices |

## Limitation

* Currently, this plugin does not support the full functionality of OrientDB. 
* It also only supports running in batch mode. 

## Usage Notes

This plugin uses one of the columns of the input record field as the 'vertex' descriptor and the edges from the vertex are represented by another column of the input record. User has the option to pick up the ability to pick the fields that represent these two concepts. 

> Vertices and Edges between them are created only if they don't exist already.
