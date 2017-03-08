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

**createIfNotExists:** Whether to create the FileSet if it does not exist. Defaults to false.

**deleteInputOnSuccess:** Whether to delete the data read if the pipeline run succeeded. Defaults to false.

**files:** A comma separated list of files in the FileSet to read. Macro enabled.

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

