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

package io.cdap.plugin.orientdb;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.batch.ConfigurationUtils;
import io.cdap.plugin.common.batch.JobUtils;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import java.util.Map;

/**
 * {@link BatchSink} plugin to write to OrientDB.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(OrientDBSink.NAME)
@Description("Batch Sink that writes to OrientDB.")
public class OrientDBSink extends BatchSink<StructuredRecord, NullWritable, StructuredRecord> {
  public static final String NAME = "OrientDB";
  public static final String ORIENTDB_CONNECTION_STRING = "orientdb.connection.string";
  public static final String ORIENTDB_USERNAME = "orientdb.username";
  public static final String ORIENTDB_PASSWORD = "orientdb.password";
  public static final String ORIENTDB_VERTEX = "orientdb.vertex";
  public static final String ORIENTDB_EDGE = "orientdb.edge";

  private final OrientDBConfig conf;

  public OrientDBSink(OrientDBConfig conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    Job job = JobUtils.createInstance();
    final Configuration hConf = job.getConfiguration();
    hConf.set(ORIENTDB_CONNECTION_STRING, conf.getConnectionString());
    hConf.set(ORIENTDB_VERTEX, conf.getVertexType());
    hConf.set(ORIENTDB_EDGE, conf.getEdgeType());
    hConf.set(ORIENTDB_USERNAME, conf.getUsername());
    hConf.set(ORIENTDB_PASSWORD, conf.getPassword());

    context.addOutput(Output.of(conf.getReferenceName(), new OutputFormatProvider() {
      @Override
      public String getOutputFormatClassName() {
        return OrientDBOutputFormat.class.getName();
      }

      @Override
      public Map<String, String> getOutputFormatConfiguration() {
        return ConfigurationUtils.getNonDefaultConfigurations(hConf);
      }
    }));

    OrientGraph graph = new OrientGraph(conf.getConnectionString(), conf.getUsername(), conf.getPassword());

    // Create Vertex Type and Edge Type if they don't exist already.
    if (graph.getVertexType(conf.getVertexType()) == null) {
      graph.createVertexType(conf.getVertexType());
    }

    if (graph.getEdgeType(conf.getEdgeType()) == null) {
      graph.createEdgeType(conf.getEdgeType());
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, StructuredRecord>> emitter)
    throws Exception {
    emitter.emit(new KeyValue<NullWritable, StructuredRecord>(null, input));
  }
}
