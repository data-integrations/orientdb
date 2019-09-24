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

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.IdUtils;

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

  public OrientDBConfig(String referenceName, String connectionString,
                        String username, String password, String vertexType, String edgeType) {
    this.referenceName = referenceName;
    this.connectionString = connectionString;
    this.username = username;
    this.password = password;
    this.vertexType = vertexType;
    this.edgeType = edgeType;
  }

  private OrientDBConfig(Builder builder) {
    this.referenceName = builder.referenceName;
    this.connectionString = builder.connectionString;
    this.username = builder.username;
    this.password = builder.password;
    this.vertexType = builder.vertexType;
    this.edgeType = builder.edgeType;
  }

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

  public void validate(FailureCollector failureCollector, Schema inputSchema) {
    try {
      IdUtils.validateId(referenceName);
    } catch (IllegalArgumentException ex) {
      failureCollector.addFailure(ex.getMessage(), null).withConfigProperty(REFERENCE_NAME);
    }

    Schema.Field vertexField = inputSchema.getField(vertexType);
    if (vertexField == null) {
      failureCollector.addFailure(String.format("Field '%s' is not present in input schema.", vertexType),
                                  null).withConfigProperty(VERTEX)
        .withInputSchemaField(VERTEX, null);
    } else {
      Schema vertexFieldSchema = vertexField.getSchema();

      if (vertexFieldSchema.isNullable()) {
        vertexFieldSchema = vertexFieldSchema.getNonNullable();
      }

      if (vertexFieldSchema.getLogicalType() != null || vertexFieldSchema.getType() != Schema.Type.STRING) {
        failureCollector.addFailure(String.format("Field '%s' must be of type 'string' but is of type '%s'.",
                                                  vertexField.getName(), vertexFieldSchema.getDisplayName()),
                                    null).withConfigProperty(VERTEX)
          .withInputSchemaField(VERTEX, null);
      }
    }

    Schema.Field edgeField = inputSchema.getField(edgeType);
    if (edgeField == null) {
      failureCollector.addFailure(String.format("Field '%s' is not present in input schema.", edgeType),
                                  null).withConfigProperty(EDGE)
        .withInputSchemaField(EDGE, null);
    } else {
      Schema edgeFieldSchema = edgeField.getSchema();

      if (edgeFieldSchema.isNullable()) {
        edgeFieldSchema = edgeFieldSchema.getNonNullable();
      }

      Schema componentSchema = (edgeFieldSchema != null && edgeFieldSchema.getType() == Schema.Type.ARRAY) ?
        edgeFieldSchema.getComponentSchema() : null;

      if (componentSchema != null && componentSchema.isNullable()) {
        componentSchema = componentSchema.getNonNullable();
      }

      if (componentSchema == null || componentSchema.getLogicalType() != null ||
        componentSchema.getType() != Schema.Type.STRING) {
        failureCollector.addFailure(String.format("Field '%s' must be of type 'array of string' but is of type '%s'.",
                                                  edgeField.getName(), edgeFieldSchema.getDisplayName()),
                                    null).withConfigProperty(EDGE)
          .withInputSchemaField(EDGE);
      }
    }
  }

  public void validateDBConnection(FailureCollector failureCollector) {
    try {
      new OrientGraph(connectionString, username, password);
    } catch (Exception ex) {
      failureCollector.addFailure(String.format("Cannot authenticate to '%s' with user name and password '%s':'%s'",
                                                connectionString, username, password), null)
        .withStacktrace(ex.getStackTrace())
        .withConfigProperty(CONNECTION_STRING)
        .withConfigProperty(USERNAME)
        .withConfigProperty(PASSWORD);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(OrientDBConfig copy) {
    return new Builder()
      .setReferenceName(copy.getReferenceName())
      .setConnectionString(copy.getConnectionString())
      .setUsername(copy.getUsername())
      .setPassword(copy.getPassword())
      .setVertexType(copy.getVertexType())
      .setEdgeType(copy.getEdgeType());
  }

  public static final class Builder {
    private String referenceName;
    private String connectionString;
    private String username;
    private String password;
    private String vertexType;
    private String edgeType;


    private Builder() {
    }

    public Builder setReferenceName(String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    public Builder setConnectionString(String connectionString) {
      this.connectionString = connectionString;
      return this;
    }

    public Builder setUsername(String username) {
      this.username = username;
      return this;
    }

    public Builder setPassword(String password) {
      this.password = password;
      return this;
    }

    public Builder setVertexType(String vertexType) {
      this.vertexType = vertexType;
      return this;
    }

    public Builder setEdgeType(String edgeType) {
      this.edgeType = edgeType;
      return this;
    }

    public OrientDBConfig build() {
      return new OrientDBConfig(this);
    }
  }
}
