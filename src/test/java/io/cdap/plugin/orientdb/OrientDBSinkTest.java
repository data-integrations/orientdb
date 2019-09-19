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

import com.orientechnologies.orient.client.remote.OEngineRemote;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link OrientDBSink}.
 */
public class OrientDBSinkTest extends HydratorTestBase {

  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "1.0.0");

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @BeforeClass
  public static void setupTestClass() throws Exception {
    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // add the data-pipeline artifact and mock plugins
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // add our plugins artifact with the data-pipeline artifact as its parent.
    // this will make our plugins available to data-pipeline.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                      parentArtifact,
                      OrientDBSink.class,
                      OEngineRemote.class);
  }

  @Test
  public void testOrientDBSink() throws Exception {
    String inputName = "mockInput";
    String outputName = "orientSink";
    String vertexType = "person";
    String edgeType = "follows";

    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputName));
    Map<String, String> sinkProperties = new HashMap<>();
    sinkProperties.put(OrientDBConfig.REFERENCE_NAME, "orientdb");
    sinkProperties.put(OrientDBConfig.CONNECTION_STRING, "remote:localhost:2424/Dyna");
    sinkProperties.put(OrientDBConfig.USERNAME, "root");
    sinkProperties.put(OrientDBConfig.PASSWORD, "password");
    sinkProperties.put(OrientDBConfig.VERTEX, vertexType);
    sinkProperties.put(OrientDBConfig.EDGE, edgeType);

    ETLStage sink = new ETLStage("sink", new ETLPlugin(OrientDBSink.NAME, BatchSink.PLUGIN_TYPE,
                                                       sinkProperties, null));
    ETLBatchConfig pipeConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    // create the pipeline
    ApplicationId appId = NamespaceId.DEFAULT.app("OrientDBPipeline");
    ApplicationManager appManager = deployApplication(appId, new AppRequest<>(APP_ARTIFACT, pipeConfig));

    // write the input
    Schema inputSchema = Schema.recordOf("schema",
                                         Schema.Field.of(vertexType, Schema.of(Schema.Type.STRING)),
                                         Schema.Field.of(edgeType, Schema.arrayOf(Schema.of(Schema.Type.STRING))));
    DataSetManager<Table> inputManager = getDataset(inputName);
    List<StructuredRecord> inputRecords = new ArrayList<>();
    inputRecords.add(StructuredRecord.builder(inputSchema)
                       .set(vertexType, "Jon")
                       .set(edgeType, new String[] {"Nitin", "Vikram"}).build());
    inputRecords.add(StructuredRecord.builder(inputSchema)
                       .set(vertexType, "Nitin")
                       .set(edgeType, new String[] {"Vikram", "Sree"}).build());
    MockSource.writeInput(inputManager, inputRecords);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForStopped(5, TimeUnit.MINUTES);
  }
}
