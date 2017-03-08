/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.hydrator;

import co.cask.cdap.api.data.format.StructuredRecord;
import com.google.common.base.Splitter;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
* {@link RecordWriter} for {@link OrientDBSink}.
*/
public class OrientDBRecordWriter extends RecordWriter<NullWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(OrientDBRecordWriter.class);

  private final String connectionString;
  private final String username;
  private final String password;
  private final String vertexName;
  private final String edgeName;
  private final String[] keyArray = new String[1];
  private final String[] valueArray = new String[1];
  private final String vertexClass;
  private final String edgeClass;

  private OrientGraph graph;
  private boolean initialized;

  public OrientDBRecordWriter(Configuration hConf) {
    this.connectionString = hConf.get(OrientDBSink.ORIENTDB_CONNECTION_STRING);
    this.username = hConf.get(OrientDBSink.ORIENTDB_USERNAME);
    this.password = hConf.get(OrientDBSink.ORIENTDB_PASSWORD);
    this.vertexName = hConf.get(OrientDBSink.ORIENTDB_VERTEX);
    this.edgeName = hConf.get(OrientDBSink.ORIENTDB_EDGE);
    this.vertexClass = String.format("class:%s", vertexName);
    this.edgeClass = String.format("class:%s", edgeName);
  }

  @Override
  public void write(NullWritable key, StructuredRecord value) throws IOException, InterruptedException {
    if (!initialized) {
      initialized = true;
      initializeRecordWriter();
    }

    String sourceVertexValue = value.get(vertexName);
    Vertex sourceVertex = getOrCreateVertex(sourceVertexValue);
    String edges = value.get(edgeName);
    Iterable<String> connections = Splitter.on(":").omitEmptyStrings().trimResults().split(edges);
    for (String connection : connections) {
      boolean addEdge = true;
      Vertex destinationVertex = getOrCreateVertex(connection);
      for (Edge outEdge : sourceVertex.getEdges(Direction.OUT, edgeName)) {
        if (outEdge.getVertex(Direction.IN).equals(destinationVertex)) {
          // An edge already exists between the two vertices, hence don't add another one.
          addEdge = false;
          break;
        }
      }

      if (addEdge) {
        graph.addEdge(edgeClass, sourceVertex, destinationVertex, edgeName);
      }
    }
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    if (graph != null) {
      try {
        graph.commit();
      } catch (Exception ex) {
        LOG.warn("Exception while trying to commit the OrientGraph operations. Trying to rollback operations.", ex);
        graph.rollback();
      }
    }
  }

  private Vertex getOrCreateVertex(String vertexValue) {
    keyArray[0] = vertexName;
    valueArray[0] = vertexValue;
    for (Vertex vertex : graph.getVertices(vertexName, keyArray, valueArray)) {
      // Return the first one. There should ideally be only one.
      return vertex;
    }
    return graph.addVertex(vertexClass, vertexName, vertexValue);
  }

  private void initializeRecordWriter() {
    graph = new OrientGraph(connectionString, username, password);
  }
}
