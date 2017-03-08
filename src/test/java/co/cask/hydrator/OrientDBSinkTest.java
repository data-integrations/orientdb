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
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import com.orientechnologies.orient.client.remote.OEngineRemote;
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
    sinkProperties.put(OrientDBConfig.CONNECTION_STRING, "remote:localhost:2424/More");
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
                                         Schema.Field.of(edgeType, Schema.of(Schema.Type.STRING)));
    DataSetManager<Table> inputManager = getDataset(inputName);
    List<StructuredRecord> inputRecords = new ArrayList<>();
    inputRecords.add(StructuredRecord.builder(inputSchema)
                       .set(vertexType, "Jon")
                       .set(edgeType, "Nitin:Vikram").build());
    inputRecords.add(StructuredRecord.builder(inputSchema)
                       .set(vertexType, "Nitin")
                       .set(edgeType, "Vikram:Sree").build());
    MockSource.writeInput(inputManager, inputRecords);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);
  }
}
