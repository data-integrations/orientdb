# OrientDB Batch Sink

Description
-----------

Writes data to an OrientDB database.

Use Case
--------

This sink is used whenever you want to write to OrientDB.

Properties
----------

**referenceName:** Reference Name for OrientDB Sink.

**connectionString:** Connection String that will be used to connect to OrientDB.

**username:** Username for the Database.

**password:** Password for the Database.

**vertex:** Column Name corresponding to Vertex Type. This column should be of String type.

**edge:** Column Name corresponding to Edge Type. This column should be an array of String type.

Example
-------

This example writes to "Test" OrientDB database. "person" column is used as Vertex key and "friend" column is used as
edge name to create outward edges from "person" vertex to all vertices in the "friend" column. Vertices and Edges are
created only if they don't already exist. Vertex Type of "person" and Edge Type "friend" are also created if they
don't exist already:

    {
        "name": "OrientDB",
        "type": "batchsink",
        "properties": {
            "referenceName": "orientdbtest",
            "connectionString": "remote:localhost:2424/Test",
            "username": "root",
            "password": "****",
            "vertexType": "person",
            "edgeType": "friend"
        }
    }

