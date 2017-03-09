# OrientDB Batch Sink

Description
-----------

Writes data to OrientDB database. It uses the Graph API of OrientDB to create vertices and edges between the vertices.
One of the columns of the record, denoted by 'vertex', represents the vertex and the edges from this vertex to
vertices mentioned in the 'edge' column are created. Vertices and Edges between them are created only if they don't
exist already.

Use Case
--------

This sink is used whenever you want to write to OrientDB using Graph API.

Properties
----------

**referenceName:** Reference Name for OrientDB Sink.

**connectionString:** Connection String that will be used to connect to OrientDB.

**username:** Username for the Database.

**password:** Password for the Database.

**vertex:** Vertex Column Name. This column should be of String type.

**edge:** Edge Column Name. This column should be an array of String type.

Example
-------

This example writes to "Test" OrientDB database. "person" column is used as Vertex key and "friend" column is used as
Edge name to create outward edges from "person" vertex to all vertices in the "friend" column. Vertices and Edges are
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
            "vertex": "person",
            "edge": "friend"
        }
    }

