package io.connect.scylladb;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

public class ScyllaDbSchemaBuilderTest {

    Map<String, String> settings;

    ScyllaDbSinkConnectorConfig config;

    ScyllaDbSchemaBuilder schemaBuilder;

    Schema schema;

    @Before
    public void before() {
        settings = new HashMap<>();
        settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, "scylladb");
        settings.put(ScyllaDbSinkConnectorConfig.MATERIALIZEDVIEW_NAME_EXTENSION_CONFIG, "by_extension");
        settings.put(
            ScyllaDbSinkConnectorConfig.MATERIALIZEDVIEW_SELECT_CONFIG,
            "column1,column2,column3,column4"
        );

        settings.put(
            ScyllaDbSinkConnectorConfig.MATERIALIZEDVIEW_WHERE_CLAUSE_CONFIG,
            "column1,column2,column3,column4"
        );

        settings.put(ScyllaDbSinkConnectorConfig.MATERIALIZEDVIEW_PARTITION_KEYS_CONFIG, "column1,column2");
        settings.put(
            ScyllaDbSinkConnectorConfig.MATERIALIZEDVIEW_CLUSTER_KEYS_CONFIG,
            "column3:asc,column4:desc"
        );

        config = new ScyllaDbSinkConnectorConfig(settings);
        schema = SchemaBuilder.struct()
            .field("column1", Schema.STRING_SCHEMA)
            .field("column2", Schema.STRING_SCHEMA)
            .field("column3", Schema.STRING_SCHEMA)
            .field("column4", Schema.STRING_SCHEMA)
            .build();

        schemaBuilder = new ScyllaDbSchemaBuilder(null, config);
    }

    @Test
    public void shouldReturnMaterializedViewQueryWhenTriggeredWithFullMaterializedViewInMaterializedViewConfig() {
        final String expected =
            "CREATE MATERIALIZED VIEW keyspaceNameTest.tableNameTest_by_extension AS "
                + "SELECT column1, column2, column3, column4 "
                + "FROM keyspaceNameTest.tableNameTest "
                + "WHERE column1 IS NOT null AND column2 IS NOT null AND column3 IS NOT null AND column4 IS NOT null "
                + "PRIMARY KEY ((column1, column2), column3, column4) "
                + "WITH CLUSTERING ORDER BY (column3 ASC, column4 DESC);";

        final String materializedViewQuery =
            schemaBuilder.createMaterializedViewQuery("keyspaceNameTest", "tableNameTest", schema);

        assertEquals(expected, materializedViewQuery);
    }

    @Test
    public void shouldReturnMVQueryWhenTriggeredWithoutMaterializedViewWhereClauseInMaterializedViewConfig() {
        final String expected =
            "CREATE MATERIALIZED VIEW keyspaceNameTest.tableNameTest_by_extension AS "
                + "SELECT column1, column2, column3, column4 "
                + "FROM keyspaceNameTest.tableNameTest "
                + "PRIMARY KEY ((column1, column2), column3, column4) "
                + "WITH CLUSTERING ORDER BY (column3 ASC, column4 DESC);";

        settings.remove(ScyllaDbSinkConnectorConfig.MATERIALIZEDVIEW_WHERE_CLAUSE_CONFIG);

        config = new ScyllaDbSinkConnectorConfig(settings);
        schemaBuilder = new ScyllaDbSchemaBuilder(null, config);

        final String materializedViewQuery =
            schemaBuilder.createMaterializedViewQuery("keyspaceNameTest", "tableNameTest", schema);

        assertEquals(expected, materializedViewQuery);
    }

    @Test
    public void shouldReturnMaterializedViewQueryWhenTriggeredWithEmptyWhereClauseInMaterializedViewConfig() {
        final String expected =
            "CREATE MATERIALIZED VIEW keyspaceNameTest.tableNameTest_by_extension AS "
                + "SELECT column1, column2, column3, column4 "
                + "FROM keyspaceNameTest.tableNameTest "
                + "PRIMARY KEY ((column1, column2), column3, column4) "
                + "WITH CLUSTERING ORDER BY (column3 ASC, column4 DESC);";

        settings.put(ScyllaDbSinkConnectorConfig.MATERIALIZEDVIEW_WHERE_CLAUSE_CONFIG, "");

        config = new ScyllaDbSinkConnectorConfig(settings);
        schemaBuilder = new ScyllaDbSchemaBuilder(null, config);

        final String materializedViewQuery =
            schemaBuilder.createMaterializedViewQuery("keyspaceNameTest", "tableNameTest", schema);

        assertEquals(expected, materializedViewQuery);
    }

    @Test
    public void shouldReturnMaterializedViewQueryWhenTriggeredWithoutPrimaryKeyInMaterializedViewConfig() {
        final String expected =
            "CREATE MATERIALIZED VIEW keyspaceNameTest.tableNameTest_by_extension AS "
                + "SELECT column1, column2, column3, column4 "
                + "FROM keyspaceNameTest.tableNameTest "
                + "WHERE column1 IS NOT null AND column2 IS NOT null AND column3 IS NOT null AND column4 IS NOT null;";

        settings.remove(ScyllaDbSinkConnectorConfig.MATERIALIZEDVIEW_PARTITION_KEYS_CONFIG);

        config = new ScyllaDbSinkConnectorConfig(settings);
        schemaBuilder = new ScyllaDbSchemaBuilder(null, config);

        final String materializedViewQuery =
            schemaBuilder.createMaterializedViewQuery("keyspaceNameTest", "tableNameTest", schema);

        assertEquals(expected, materializedViewQuery);
    }

    @Test
    public void shouldReturnMaterializedViewQueryWhenTriggeredWithEmptyPrimaryKeyInMaterializedViewConfig() {
        final String expected =
            "CREATE MATERIALIZED VIEW keyspaceNameTest.tableNameTest_by_extension AS "
                + "SELECT column1, column2, column3, column4 "
                + "FROM keyspaceNameTest.tableNameTest "
                + "WHERE column1 IS NOT null AND column2 IS NOT null AND column3 IS NOT null AND column4 IS NOT null;";

        settings.put(ScyllaDbSinkConnectorConfig.MATERIALIZEDVIEW_PARTITION_KEYS_CONFIG, "");

        config = new ScyllaDbSinkConnectorConfig(settings);
        schemaBuilder = new ScyllaDbSchemaBuilder(null, config);

        final String materializedViewQuery =
            schemaBuilder.createMaterializedViewQuery("keyspaceNameTest", "tableNameTest", schema);

        assertEquals(expected, materializedViewQuery);
    }

    @Test
    public void shouldReturnMVQueryWhenTriggeredWithoutWhereClauseAndPrimaryKeyInMaterializedViewConfig() {
        final String expected =
            "CREATE MATERIALIZED VIEW keyspaceNameTest.tableNameTest_by_extension AS "
                + "SELECT column1, column2, column3, column4 "
                + "FROM keyspaceNameTest.tableNameTest;";

        settings.remove(
            ScyllaDbSinkConnectorConfig.MATERIALIZEDVIEW_WHERE_CLAUSE_CONFIG
        );

        settings.remove(
            ScyllaDbSinkConnectorConfig.MATERIALIZEDVIEW_PARTITION_KEYS_CONFIG
        );

        config = new ScyllaDbSinkConnectorConfig(settings);
        schemaBuilder = new ScyllaDbSchemaBuilder(null, config);

        final String materializedViewQuery =
            schemaBuilder.createMaterializedViewQuery("keyspaceNameTest", "tableNameTest", schema);

        assertEquals(expected, materializedViewQuery);
    }

    @Test
    public void shouldReturnMaterializedViewQueryWhenTriggeredWithoutClusterKeyInMaterializedViewConfig() {
        final String expected =
            "CREATE MATERIALIZED VIEW keyspaceNameTest.tableNameTest_by_extension AS "
                + "SELECT column1, column2, column3, column4 "
                + "FROM keyspaceNameTest.tableNameTest "
                + "WHERE column1 IS NOT null AND column2 IS NOT null AND column3 IS NOT null AND column4 IS NOT null "
                + "PRIMARY KEY ((column1, column2));";

        settings.remove(
            ScyllaDbSinkConnectorConfig.MATERIALIZEDVIEW_CLUSTER_KEYS_CONFIG
        );

        config = new ScyllaDbSinkConnectorConfig(settings);
        schemaBuilder = new ScyllaDbSchemaBuilder(null, config);

        final String materializedViewQuery =
            schemaBuilder.createMaterializedViewQuery("keyspaceNameTest", "tableNameTest", schema);

        assertEquals(expected, materializedViewQuery);
    }

    @Test
    public void shouldReturnMaterializedViewQueryWhenTriggeredWithEmptyClusterKeyInMaterializedViewConfig() {
        final String expected =
            "CREATE MATERIALIZED VIEW keyspaceNameTest.tableNameTest_by_extension AS "
                + "SELECT column1, column2, column3, column4 "
                + "FROM keyspaceNameTest.tableNameTest "
                + "WHERE column1 IS NOT null AND column2 IS NOT null AND column3 IS NOT null AND column4 IS NOT null "
                + "PRIMARY KEY ((column1, column2));";

        settings.put(ScyllaDbSinkConnectorConfig.MATERIALIZEDVIEW_CLUSTER_KEYS_CONFIG, "");

        config = new ScyllaDbSinkConnectorConfig(settings);
        schemaBuilder = new ScyllaDbSchemaBuilder(null, config);

        final String materializedViewQuery =
            schemaBuilder.createMaterializedViewQuery("keyspaceNameTest", "tableNameTest", schema);

        assertEquals(expected, materializedViewQuery);
    }

    @Test
    public void shouldReturnMaterializedViewQueryWhenTriggeredWithSinglePrimaryKeyInMaterializedViewConfig() {
        final String expected =
            "CREATE MATERIALIZED VIEW keyspaceNameTest.tableNameTest_by_extension AS "
                + "SELECT column1, column2, column3, column4 "
                + "FROM keyspaceNameTest.tableNameTest "
                + "WHERE column1 IS NOT null AND column2 IS NOT null AND column3 IS NOT null AND column4 IS NOT null "
                + "PRIMARY KEY (column1, column3, column4) "
                + "WITH CLUSTERING ORDER BY (column3 ASC, column4 DESC);";

        settings.put(ScyllaDbSinkConnectorConfig.MATERIALIZEDVIEW_PARTITION_KEYS_CONFIG, "column1");

        config = new ScyllaDbSinkConnectorConfig(settings);
        schemaBuilder = new ScyllaDbSchemaBuilder(null, config);

        final String materializedViewQuery =
            schemaBuilder.createMaterializedViewQuery("keyspaceNameTest", "tableNameTest", schema);

        assertEquals(expected, materializedViewQuery);
    }

    @Test
    public void shouldReturnMVQueryWhenTriggeredWithSinglePrimaryKeyAndWithoutClusterKeyInMaterializedViewConfig() {
        final String expected =
            "CREATE MATERIALIZED VIEW keyspaceNameTest.tableNameTest_by_extension AS "
                + "SELECT column1, column2, column3, column4 "
                + "FROM keyspaceNameTest.tableNameTest "
                + "WHERE column1 IS NOT null AND column2 IS NOT null AND column3 IS NOT null AND column4 IS NOT null "
                + "PRIMARY KEY (column1);";

        settings.put(ScyllaDbSinkConnectorConfig.MATERIALIZEDVIEW_PARTITION_KEYS_CONFIG, "column1");
        settings.remove(
            ScyllaDbSinkConnectorConfig.MATERIALIZEDVIEW_CLUSTER_KEYS_CONFIG
        );

        config = new ScyllaDbSinkConnectorConfig(settings);
        schemaBuilder = new ScyllaDbSchemaBuilder(null, config);

        final String materializedViewQuery =
            schemaBuilder.createMaterializedViewQuery("keyspaceNameTest", "tableNameTest", schema);

        assertEquals(expected, materializedViewQuery);
    }

    @Test
    public void shouldReturnMaterializedQueryViewWithAllSelectInMaterializedViewConfig() {
        final String expected =
            "CREATE MATERIALIZED VIEW keyspaceNameTest.tableNameTest_by_extension AS "
                + "SELECT * "
                + "FROM keyspaceNameTest.tableNameTest "
                + "WHERE column1 IS NOT null AND column2 IS NOT null AND column3 IS NOT null AND column4 IS NOT null "
                + "PRIMARY KEY ((column1, column2), column3, column4) "
                + "WITH CLUSTERING ORDER BY (column3 ASC, column4 DESC);";

        settings.put(ScyllaDbSinkConnectorConfig.MATERIALIZEDVIEW_SELECT_CONFIG, "*");

        config = new ScyllaDbSinkConnectorConfig(settings);
        schemaBuilder = new ScyllaDbSchemaBuilder(null, config);

        final String materializedViewQuery =
            schemaBuilder.createMaterializedViewQuery("keyspaceNameTest", "tableNameTest", schema);

        assertEquals(expected, materializedViewQuery);
    }

    @Test(expected = ConfigException.class)
    public void shouldThrowConfigExceptionWithoutSelectInMaterializedViewConfig() {
        settings.remove(ScyllaDbSinkConnectorConfig.MATERIALIZEDVIEW_SELECT_CONFIG);

        config = new ScyllaDbSinkConnectorConfig(settings);
        schemaBuilder = new ScyllaDbSchemaBuilder(null, config);

        schemaBuilder.createMaterializedViewQuery("keyspaceNameTest", "tableNameTest", schema);
    }

    @Test(expected = ConfigException.class)
    public void shouldThrowConfigExceptionWithEmptySelectInMaterializedViewConfig() {
        settings.put(ScyllaDbSinkConnectorConfig.MATERIALIZEDVIEW_SELECT_CONFIG, "");

        config = new ScyllaDbSinkConnectorConfig(settings);
        schemaBuilder = new ScyllaDbSchemaBuilder(null, config);

        schemaBuilder.createMaterializedViewQuery("keyspaceNameTest", "tableNameTest", schema);
    }

    @Test(expected = ConfigException.class)
    public void shouldThrowConfigExceptionWithInvalidSelectInMaterializedViewConfig() {
        settings.put(ScyllaDbSinkConnectorConfig.MATERIALIZEDVIEW_SELECT_CONFIG, "not_column_name");

        config = new ScyllaDbSinkConnectorConfig(settings);
        schemaBuilder = new ScyllaDbSchemaBuilder(null, config);

        schemaBuilder.createMaterializedViewQuery("keyspaceNameTest", "tableNameTest", schema);
    }

    @Test(expected = ConfigException.class)
    public void shouldThrowConfigExceptionWithInvalidMaterializedViewNameSelectInMaterializedViewConfig() {
        schemaBuilder.createMaterializedViewQuery("keyspaceNameTest", "123tableNameTest", schema);
    }

    @Test(expected = ConfigException.class)
    public void shouldThrowConfigExceptionWithInvalidWhereInMaterializedViewConfig() {
        settings.put(ScyllaDbSinkConnectorConfig.MATERIALIZEDVIEW_WHERE_CLAUSE_CONFIG, "not_column_name");

        config = new ScyllaDbSinkConnectorConfig(settings);
        schemaBuilder = new ScyllaDbSchemaBuilder(null, config);

        schemaBuilder.createMaterializedViewQuery("keyspaceNameTest", "tableNameTest", schema);
    }

    @Test(expected = ConfigException.class)
    public void shouldThrowConfigExceptionWithInvalidPrimaryKeyInMaterializedViewConfig() {
        settings.put(ScyllaDbSinkConnectorConfig.MATERIALIZEDVIEW_PARTITION_KEYS_CONFIG, "not_column_name");

        config = new ScyllaDbSinkConnectorConfig(settings);
        schemaBuilder = new ScyllaDbSchemaBuilder(null, config);

        schemaBuilder.createMaterializedViewQuery("keyspaceNameTest", "tableNameTest", schema);
    }

    @Test(expected = ConfigException.class)
    public void shouldThrowConfigExceptionWithClusterKeyNotContainColonInMaterializedViewConfig() {
        settings.put(ScyllaDbSinkConnectorConfig.MATERIALIZEDVIEW_CLUSTER_KEYS_CONFIG, "not_column_name");

        config = new ScyllaDbSinkConnectorConfig(settings);
        schemaBuilder = new ScyllaDbSchemaBuilder(null, config);

        schemaBuilder.createMaterializedViewQuery("keyspaceNameTest", "tableNameTest", schema);
    }

    @Test(expected = ConfigException.class)
    public void shouldThrowConfigExceptionWithInvalidClusterKeyDirectionInMaterializedViewConfig() {
        settings.put(ScyllaDbSinkConnectorConfig.MATERIALIZEDVIEW_CLUSTER_KEYS_CONFIG, "column4:not_desc");

        config = new ScyllaDbSinkConnectorConfig(settings);
        schemaBuilder = new ScyllaDbSchemaBuilder(null, config);

        schemaBuilder.createMaterializedViewQuery("keyspaceNameTest", "tableNameTest", schema);
    }

    @Test(expected = ConfigException.class)
    public void shouldThrowConfigExceptionWithInvalidClusterKeyInMaterializedViewConfig() {
        settings.put(ScyllaDbSinkConnectorConfig.MATERIALIZEDVIEW_CLUSTER_KEYS_CONFIG, "not_column:desc");

        config = new ScyllaDbSinkConnectorConfig(settings);
        schemaBuilder = new ScyllaDbSchemaBuilder(null, config);

        schemaBuilder.createMaterializedViewQuery("keyspaceNameTest", "tableNameTest", schema);
    }
}
