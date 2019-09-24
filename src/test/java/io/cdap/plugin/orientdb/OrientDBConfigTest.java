package io.cdap.plugin.orientdb;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class OrientDBConfigTest {
  private static final String MOCK_STAGE = "mockStage";
  private static final Schema VALID_SCHEMA =
    Schema.recordOf("schema",
                    Schema.Field.of("MyV", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("MyE",
                                    Schema.nullableOf(Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING
                                    ))))));

  private static final OrientDBConfig VALID_CONFIG = new OrientDBConfig(
    "OrientDBSource",
    null,
    null,
    null,
    "MyV",
    "MyE"
  );

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector, VALID_SCHEMA);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testInvalidReferenceName() {
    OrientDBConfig config = OrientDBConfig.builder(VALID_CONFIG)
      .setReferenceName("!@#$%^&*()")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, VALID_SCHEMA);
    assertValidationFailed(failureCollector, OrientDBConfig.REFERENCE_NAME);
  }

  @Test
  public void testVertexAbsent() {
    OrientDBConfig config = OrientDBConfig.builder(VALID_CONFIG)
      .setVertexType("nonExistingField")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, VALID_SCHEMA);
    assertValidationFailed(failureCollector, OrientDBConfig.VERTEX);
  }

  @Test
  public void testEdgeAbsent() {
    OrientDBConfig config = OrientDBConfig.builder(VALID_CONFIG)
      .setEdgeType("nonExistingField")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, VALID_SCHEMA);
    assertValidationFailed(failureCollector, OrientDBConfig.EDGE);
  }

  @Test
  public void testVertexInvalidType() {
    Schema schema =
      Schema.recordOf("schema",
                      Schema.Field.of("MyV", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
                      Schema.Field.of("MyE",
                                      Schema.nullableOf(Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING
                                      ))))));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector, schema);
    assertValidationFailed(failureCollector, OrientDBConfig.VERTEX);
  }

  @Test
  public void testEdgeInvalidType() {
    Schema schema =
      Schema.recordOf("schema",
                      Schema.Field.of("MyV", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of("MyE",
                                      Schema.nullableOf(Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.BYTES
                                      ))))));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector, schema);
    assertValidationFailed(failureCollector, OrientDBConfig.EDGE);
  }

  private static void assertValidationFailed(MockFailureCollector failureCollector, String paramName) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();

    Assert.assertEquals(1, failureList.size());
    ValidationFailure failure = failureList.get(0);
    List<ValidationFailure.Cause> causeList = getCauses(failure, CauseAttributes.STAGE_CONFIG);
    Assert.assertEquals(1, causeList.size());
    ValidationFailure.Cause cause = causeList.get(0);
    Assert.assertEquals(paramName, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Nonnull
  private static List<ValidationFailure.Cause> getCauses(ValidationFailure failure, String stacktrace) {
    return failure.getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(stacktrace) != null)
      .collect(Collectors.toList());
  }
}
