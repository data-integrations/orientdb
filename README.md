# OrientDB Batch Sink

<a href="https://cdap-users.herokuapp.com/"><img alt="Join CDAP community" src="https://cdap-users.herokuapp.com/badge.svg?t=1"/></a> [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

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

# Build
To build this plugin:

```
   mvn clean package
```    

The build will create a .jar and .json file under the ``target`` directory.
These files can be used to deploy your plugins.

Deployment
----------
You can deploy your plugins using the CDAP CLI:

    > load artifact <target/orientdb-batch-sink.jar> config-file <target/orientdb-batch-sink.json>

For example, if your artifact is named 'kudu-sink-1.0.0':

    > load artifact target/orientdb-batch-sink-1.0.0.jar config-file target/orientdb-batch-sink-1.0.0.json
    
# Mailing Lists

CDAP User Group and Development Discussions:

* `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from 
users, release announcements, and any other discussions that we think will be helpful 
to the users.

## IRC Channel

CDAP IRC Channel: #cdap on irc.freenode.net


# License and Trademarks

Copyright © 2016-2017 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the 
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
either express or implied. See the License for the specific language governing permissions 
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.
