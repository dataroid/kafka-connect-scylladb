package io.connect.scylladb;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.SchemaChangeListenerBase;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.schemabuilder.Alter;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ComparisonChain;
import io.connect.scylladb.topictotable.TopicConfigs;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ScyllaDbSchemaBuilder extends SchemaChangeListenerBase {

  private static final Logger log = LoggerFactory.getLogger(ScyllaDbSchemaBuilder.class);

  private static final Object DEFAULT = new Object();

  private final ScyllaDbSinkConnectorConfig config;
  private final Cache<ScyllaDbSchemaKey, Object> schemaLookup;

  final ScyllaDbSession session;

  public ScyllaDbSchemaBuilder(ScyllaDbSession session, ScyllaDbSinkConnectorConfig config) {
    this.session = session;
    this.config = config;
    this.schemaLookup = CacheBuilder.newBuilder()
        .expireAfterWrite(500L, TimeUnit.SECONDS)
        .build();
  }

  @Override
  public void onTableChanged(com.datastax.driver.core.TableMetadata current, com.datastax.driver.core.TableMetadata previous) {
    final com.datastax.driver.core.TableMetadata actual;
    if (null != current) {
      actual = current;
    } else if (null != previous) {
      actual = previous;
    } else {
      actual = null;
    }

    if (null != actual) {
      final String keyspace = actual.getKeyspace().getName();
      if (this.config.keyspace.equalsIgnoreCase(keyspace)) {
        ScyllaDbSchemaKey key =
            ScyllaDbSchemaKey.of(actual.getKeyspace().getName(), actual.getName());
        log.info("onTableChanged() - {} changed. Invalidating...", key);
        this.schemaLookup.invalidate(key);
        this.session.onTableChanged(actual.getKeyspace().getName(), actual.getName());
      }
    }
  }

  DataType dataType(Schema schema) {
    final DataType dataType;

    if (Timestamp.LOGICAL_NAME.equals(schema.name())) {
      dataType = DataType.timestamp();
    } else if (Time.LOGICAL_NAME.equals(schema.name())) {
      dataType = DataType.time();
    } else if (Date.LOGICAL_NAME.equals(schema.name())) {
      dataType = DataType.date();
    } else if (Decimal.LOGICAL_NAME.equals(schema.name())) {
      dataType = DataType.decimal();
    } else if ("com.dataroid.connect.data.UUID".equals(schema.name())) {
      dataType = DataType.uuid();
    } else if ("com.dataroid.connect.data.TEXT".equals(schema.name())) {
      dataType = DataType.text();
    } else {
      switch (schema.type()) {
        case MAP:
          final DataType mapKeyType = dataType(schema.keySchema());
          final DataType mapValueType = dataType(schema.valueSchema());
          dataType = DataType.map(mapKeyType, mapValueType);
          break;
        case ARRAY:
          final DataType listValueType = dataType(schema.valueSchema());
          dataType = DataType.list(listValueType);
          break;
        case BOOLEAN:
          dataType = DataType.cboolean();
          break;
        case BYTES:
          dataType = DataType.blob();
          break;
        case FLOAT32:
          dataType = DataType.cfloat();
          break;
        case FLOAT64:
          dataType = DataType.cdouble();
          break;
        case INT8:
          dataType = DataType.tinyint();
          break;
        case INT16:
          dataType = DataType.smallint();
          break;
        case INT32:
          dataType = DataType.cint();
          break;
        case INT64:
          dataType = DataType.bigint();
          break;
        case STRING:
          dataType = DataType.varchar();
          break;
        default:
          throw new DataException(
              String.format("Unsupported type %s", schema.type())
          );
      }
    }
    return dataType;
  }

  void alter(
          final ScyllaDbSchemaKey key,
          String keyspace,
          String tableName,
          SinkRecord record,
          TableMetadata.Table tableMetadata,
          TopicConfigs topicConfigs
  ) {
    Preconditions.checkNotNull(tableMetadata, "tableMetadata cannot be null.");
    Preconditions.checkNotNull(record.valueSchema(), "valueSchema cannot be null.");
    log.trace("alter() - tableMetadata = '{}' ", tableMetadata);

    Map<String, DataType> addedColumns = new LinkedHashMap<>();

    if (topicConfigs != null && topicConfigs.isScyllaColumnsMapped()) {
      if (topicConfigs.getTablePartitionKeyMap().keySet().size() != tableMetadata.primaryKey().size()) {
        throw new DataException(
                String.format(
                        "Cannot alter primary key of a ScyllaDb Table. Existing primary key: '%s', "
                                + "Primary key mapped in 'topic.my_topic.my_ks.my_table.mapping' config: '%s",
                        Joiner.on("', '").join(tableMetadata.primaryKey()),
                        Joiner.on("', '").join(topicConfigs.getTablePartitionKeyMap().keySet())
                )
        );
      }

      for (Map.Entry<String, TopicConfigs.KafkaScyllaColumnMapper> entry: topicConfigs.getTableColumnMap().entrySet()) {
        String columnName = entry.getValue().getScyllaColumnName();
        log.trace("alter for mapping() - Checking if table has '{}' column.", columnName);
        final TableMetadata.Column columnMetadata = tableMetadata.columnMetadata(columnName);

        if (null == columnMetadata) {
          log.debug("alter for mapping() - Adding column '{}'", columnName);
          final DataType dataType = dataType(entry.getValue().getKafkaRecordField().schema());
          addedColumns.put(columnName, dataType);
        } else {
          log.trace("alter for mapping() - Table already has '{}' column.", columnName);
        }
      }
    } else {
      for (final Field field : record.valueSchema().fields()) {
        log.trace("alter() - Checking if table has '{}' column.", field.name());
        final TableMetadata.Column columnMetadata = tableMetadata.columnMetadata(field.name());

        if (null == columnMetadata) {
          log.debug("alter() - Adding column '{}'", field.name());
          DataType dataType = dataType(field.schema());
          addedColumns.put(field.name(), dataType);
        } else {
          log.trace("alter() - Table already has '{}' column.", field.name());
        }
      }
    }

    /*
    ScyllaDb is a little weird. It will not allow more than one column in an alter statement. It
    looks like this is a limitation of CQL in general. Check out this issue for more.
    https://datastax-oss.atlassian.net/browse/JAVA-731
     */

    if (!addedColumns.isEmpty()) {
      final Alter alterTable = SchemaBuilder.alterTable(keyspace, tableName);
      if (!this.config.tableManageEnabled) {
        List<String> requiredAlterStatements = addedColumns.entrySet().stream()
                .map(e -> alterTable.addColumn(e.getKey()).type(e.getValue()).toString())
                .collect(Collectors.toList());

        throw new DataException(
                String.format(
                        "Alter statement(s) needed. Missing column(s): '%s'\n%s;",
                        Joiner.on("', '").join(addedColumns.keySet()),
                        Joiner.on(';').join(requiredAlterStatements)
                )
        );
      } else {
        String query = alterTable.withOptions()
                .compressionOptions(config.tableCompressionAlgorithm).buildInternal();
        this.session.executeQuery(query);
        for (Map.Entry<String, DataType> e : addedColumns.entrySet()) {
          final String columnName = e.getKey();
          final DataType dataType = e.getValue();
          final Statement alterStatement = alterTable.addColumn(columnName).type(dataType);
          this.session.executeStatement(alterStatement);
        }
        this.session.onTableChanged(keyspace, tableName);
      }
    }

    this.schemaLookup.put(key, DEFAULT);
  }

  public void build(String keyspace, String tableName, SinkRecord record, TopicConfigs topicConfigs) {
    log.trace("build() - tableName = '{}.{}'", keyspace, tableName);
    final ScyllaDbSchemaKey key = ScyllaDbSchemaKey.of(keyspace, tableName);
    if (null != this.schemaLookup.getIfPresent(key)) {
      return;
    }
    if (null == record.keySchema() || null == record.valueSchema()) {
      log.warn(
              "build() - Schemaless mode detected. Cannot generate DDL so assuming table is correct."
      );
      this.schemaLookup.put(key, DEFAULT);
      return;
    }

    final TableMetadata.Table tableMetadata = this.session.tableMetadata(keyspace, tableName);

    if (null != tableMetadata) {
      alter(key, keyspace, tableName, record, tableMetadata, topicConfigs);
    } else {
      create(key, keyspace, tableName, record, topicConfigs);
    }
  }

  void create(
          final ScyllaDbSchemaKey key,
          String keyspace,
          String tableName,
          SinkRecord record,
          TopicConfigs topicConfigs
  ) {
    Schema keySchema = record.keySchema();
    Schema valueSchema = record.valueSchema();
    log.trace("create() - tableName = '{}'", tableName);
    Preconditions.checkState(
            Schema.Type.STRUCT == keySchema.type(),
            "record.keySchema() must be a struct. Received '%s'",
            keySchema.type()
    );
    Preconditions.checkState(
            !keySchema.fields().isEmpty(),
            "record.keySchema() must have some fields."
    );
    if (topicConfigs != null && topicConfigs.isScyllaColumnsMapped()) {
      Preconditions.checkState(
              Schema.Type.STRUCT == valueSchema.type(),
              "record.valueSchema() must be a struct. Received '%s'",
              valueSchema.type()
      );
      Preconditions.checkState(
              !valueSchema.fields().isEmpty(),
              "record.valueSchema() must have some fields."
      );
    } else {
      for (final Field keyField : keySchema.fields()) {
        log.trace(
                "create() - Checking key schema against value schema. fieldName={}",
                keyField.name()
        );
        final Field valueField = valueSchema.field(keyField.name());

        if (null == valueField) {
          throw new DataException(
                  String.format(
                          "record.valueSchema() must contain all of the fields in record.keySchema(). "
                                  + "record.keySchema() is used by the connector to determine the key for the "
                                  + "table. record.valueSchema() is missing field '%s'. record.valueSchema() is "
                                  + "used by the connector to persist data to the table in ScyllaDb. Here are "
                                  + "the available fields for record.valueSchema(%s) and record.keySchema(%s).",
                          keyField.name(),
                          Joiner.on(", ").join(
                                  valueSchema.fields().stream().map(Field::name).collect(Collectors.toList())
                          ),
                          Joiner.on(", ").join(
                                  keySchema.fields().stream().map(Field::name).collect(Collectors.toList())
                          )
                  )
          );
        }
      }
    }

    Create create = SchemaBuilder.createTable(keyspace, tableName);
    final Create.Options tableOptions = create.withOptions();
    if (!Strings.isNullOrEmpty(valueSchema.doc())) {
      tableOptions.comment(valueSchema.doc());
    }
    if (topicConfigs != null && topicConfigs.isScyllaColumnsMapped()) {
      for (Map.Entry<String, TopicConfigs.KafkaScyllaColumnMapper> entry: topicConfigs.getTablePartitionKeyMap().entrySet()) {
        final DataType dataType = dataType(entry.getValue().getKafkaRecordField().schema());
        create.addPartitionKey(entry.getValue().getScyllaColumnName(), dataType);
      }
      for (Map.Entry<String, TopicConfigs.KafkaScyllaColumnMapper> entry: topicConfigs.getTableColumnMap().entrySet()) {
        final DataType dataType = dataType(entry.getValue().getKafkaRecordField().schema());
        create.addColumn(entry.getValue().getScyllaColumnName(), dataType);
      }
    } else {
      Set<String> fields = new HashSet<>();
      for (final Field keyField : keySchema.fields()) {
        final DataType dataType = dataType(keyField.schema());
        create.addPartitionKey(keyField.name(), dataType);
        fields.add(keyField.name());
      }

      final Header clusterKeys = record.headers().lastWithName("table.clusterKeys");

      if (Objects.nonNull(clusterKeys)) {
        final Map<String, Schema> keys = (Map<String, Schema>) clusterKeys.value();

        for (Map.Entry<String, Schema> entry : keys.entrySet()) {
          fields.add(entry.getKey());
          final DataType dataType = dataType(entry.getValue().schema());
          create.addClusteringColumn(entry.getKey(), dataType);
        }
      }

      final Header clusterKeysDirections = record.headers().lastWithName("table.clusterKeysDirections");

      if (Objects.nonNull(clusterKeysDirections)) {
        final Map<String, String> directions = (Map<String, String>) clusterKeysDirections.value();

        for (Map.Entry<String, String> entry : directions.entrySet()) {
          if (entry.getValue().equals(SchemaBuilder.Direction.DESC.name())) {
            tableOptions.clusteringOrder(entry.getKey(), SchemaBuilder.Direction.DESC);
          } else if (entry.getValue().equals(SchemaBuilder.Direction.ASC.name())) {
            tableOptions.clusteringOrder(entry.getKey(), SchemaBuilder.Direction.ASC);
          }
        }
      }

      for (final Field valueField : valueSchema.fields()) {
        if (fields.contains(valueField.name())) {
          log.trace("create() - Skipping '{}' because it's already in the key.", valueField.name());
          continue;
        }

        final DataType dataType = dataType(valueField.schema());
        create.addColumn(valueField.name(), dataType);
      }
    }

    if (this.config.tableManageEnabled) {
      tableOptions.compressionOptions(config.tableCompressionAlgorithm).buildInternal();
      log.info("create() - Adding table {}.{}\n{}", keyspace, tableName, tableOptions);
      session.executeStatement(tableOptions);

      final String materializedViewQuery = createMaterializedViewQuery(keyspace, tableName, record.valueSchema());
      log.info("create() - Materialized view : {}", materializedViewQuery);
      session.executeQuery(createMaterializedViewQuery(keyspace, tableName, record.valueSchema()));
    } else {
      throw new DataException(
              String.format("Create statement needed:\n%s", create)
      );
    }
    this.schemaLookup.put(key, DEFAULT);
  }

  public String createMaterializedViewQuery(final String keyspace, final String tableName, final Schema valueSchema) {
    String materializedViewName = tableName + "_" + this.config.mvNameExtension;
    materializedViewName = materializedViewName.replaceAll("\\W", "");

    final Pattern materializedViewNamePattern = Pattern.compile("^[a-zA-Z][a-zA-Z0-9\\_]+$");
    final Matcher matcherToValidate = materializedViewNamePattern.matcher(materializedViewName);

    if (!matcherToValidate.matches()) {
      throw new DataException(String.format("Materialized view name (%s) is not valid.", materializedViewName));
    }

    final List<String> selectColumns = this.config.mvSelectColumns;

    if (selectColumns.size() == 0) {
      throw new ConfigException("There needs to be selected columns to create materialized view.");
    }

    if (!(selectColumns.size() == 1 && selectColumns.get(0).equals("*"))) {
      selectColumns.forEach(selectColumn -> {
        if (Objects.isNull(valueSchema.field(selectColumn))) {
          throw new ConfigException(String.format(
              "The select column (%s) is not defined in value schema",
              selectColumn
          ));
        }
      });
    }

    final String selectStatement = Joiner.on(", ").join(selectColumns);

    List<String> whereClauseItems = this.config.mvWhereClauseProperties;
    whereClauseItems.forEach(whereClauseItem -> {
      if (Objects.isNull(valueSchema.field(whereClauseItem))) {
        throw new ConfigException(String.format(
            "The where clause item (%s) is not defined in value schema",
            whereClauseItem
        ));
      }
    });

    whereClauseItems = whereClauseItems.stream().map(where -> {
      where = where + " IS NOT null";

      return where;
    }).collect(Collectors.toList());

    final String whereStatement = Joiner.on(" AND ").join(whereClauseItems);

    final List<String> partitionKeys = this.config.mvPartitionKeys;
    partitionKeys.forEach(partitionKey -> {
      if (Objects.isNull(valueSchema.field(partitionKey))) {
        throw new ConfigException(String.format("The partition key (%s) is not defined in value schema", partitionKey));
      }
    });

    final String partitionKeyStatement = Joiner.on(", ").join(partitionKeys);

    final Map<String, String> clusterKeysPropertyMap = new LinkedHashMap<>();
    this.config.mvClusterKeys.forEach(clusterKeyProperties -> {
      final String[] parts = clusterKeyProperties.split(":");

      if (parts.length != 2) {
        throw new ConfigException(String.format("The cluster keys config (%s) is invalid.", clusterKeyProperties));
      }

      clusterKeysPropertyMap.put(parts[0], parts[1]);
    });

    clusterKeysPropertyMap.keySet().forEach(clusterKey -> {
      if (Objects.isNull(valueSchema.field(clusterKey))) {
        throw new ConfigException(String.format("The cluster key (%s) is not defined in value schema", clusterKey));
      }
    });

    final String clusterKeyStatement = Joiner.on(", ").join(clusterKeysPropertyMap.keySet());

    final List<String> clusterKeysDirections = clusterKeysPropertyMap
        .entrySet()
        .stream()
        .map(clusterKeysProperty -> {
              String direction = "ASC";

              if (clusterKeysProperty.getValue().equals("desc")) {
                direction = "DESC";
              }

              return clusterKeysProperty.getKey() + " " + direction;
            }
        ).collect(Collectors.toList());

    final String clusterKeyDirectionStatement = Joiner.on(", ").join(clusterKeysDirections);

    final String materializedViewQuery =
        "CREATE MATERIALIZED VIEW %s.%s AS "
            + "SELECT %s "
            + "FROM %s.%s "
            + "WHERE %s "
            + "PRIMARY KEY ((%s), %s) "
            + "WITH CLUSTERING ORDER BY (%s);";

    return String.format(
        materializedViewQuery,
        keyspace,
        materializedViewName,
        selectStatement,
        keyspace,
        tableName,
        whereStatement,
        partitionKeyStatement,
        clusterKeyStatement,
        clusterKeyDirectionStatement
    );
  }

  static class ScyllaDbSchemaKey implements Comparable<ScyllaDbSchemaKey> {
    final String tableName;
    final String keyspace;


    private ScyllaDbSchemaKey(String keyspace, String tableName) {
      this.tableName = tableName;
      this.keyspace = keyspace;
    }


    @Override
    public int compareTo(ScyllaDbSchemaKey that) {
      return ComparisonChain.start()
          .compare(this.keyspace, that.keyspace)
          .compare(this.tableName, that.tableName)
          .result();
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.keyspace, this.tableName);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("keyspace", this.keyspace)
          .add("tableName", this.tableName)
          .toString();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof ScyllaDbSchemaKey) {
        return 0 == compareTo((ScyllaDbSchemaKey) obj);
      } else {
        return false;
      }
    }

    public static ScyllaDbSchemaKey of(String keyspace, String tableName) {
      return new ScyllaDbSchemaKey(keyspace, tableName);
    }
  }

}
