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
import io.cdap.cdap.api.plugin.PluginConfig;

/**
* {@link PluginConfig} for {@link OrientDBSink}.
*/
public class OrientDBConfig extends PluginConfig {
  public static final String REFERENCE_NAME = "referenceName";
  public static final String CONNECTION_STRING = "connectionString";
  public static final String USERNAME = "username";
  public static final String PASSWORD = "password";
  public static final String VERTEX = "vertex";
  public static final String EDGE = "edge";

  @Name(REFERENCE_NAME)
  @Description("Reference Name for the OrientDB Sink")
  private String referenceName;

  @Name(CONNECTION_STRING)
  @Description("Specific the OrientDB connection string. Example: 'remote:localhost:2424/DBName'")
  private String connectionString;

  @Name(USERNAME)
  @Description("OrientDB Username")
  private String username;

  @Name(PASSWORD)
  @Description("OrientDB Password")
  private String password;

  @Name(VERTEX)
  @Description("Column Name corresponding to Vertex Type. This column should be of String type.")
  private String vertexType;

  @Name(EDGE)
  @Description("Column Name corresponding to Edge Type. This column should be an array of String type.")
  private String edgeType;

  public String getReferenceName() {
    return referenceName;
  }

  public String getConnectionString() {
    return connectionString;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getVertexType() {
    return vertexType;
  }

  public String getEdgeType() {
    return edgeType;
  }
}
