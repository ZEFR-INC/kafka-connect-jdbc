/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.dialect;

import com.mockrunner.mock.jdbc.MockConnection;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.Test;
import org.postgresql.util.PGobject;

import java.io.IOException;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import io.confluent.connect.jdbc.source.ColumnMapping;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.TableId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PostgreSqlDatabaseDialectTest extends BaseDialectTest<PostgreSqlDatabaseDialect> {

  @Override
  protected PostgreSqlDatabaseDialect createDialect() {
    return new PostgreSqlDatabaseDialect(sourceConfigWithUrl("jdbc:postgresql://something"));
  }

  @Test
  public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
    assertPrimitiveMapping(Type.INT8, "SMALLINT");
    assertPrimitiveMapping(Type.INT16, "SMALLINT");
    assertPrimitiveMapping(Type.INT32, "INT");
    assertPrimitiveMapping(Type.INT64, "BIGINT");
    assertPrimitiveMapping(Type.FLOAT32, "REAL");
    assertPrimitiveMapping(Type.FLOAT64, "DOUBLE PRECISION");
    assertPrimitiveMapping(Type.BOOLEAN, "BOOLEAN");
    assertPrimitiveMapping(Type.BYTES, "BYTEA");
    assertPrimitiveMapping(Type.STRING, "TEXT");
  }

  @Test
  public void shouldMapDecimalSchemaTypeToDecimalSqlType() {
    assertDecimalMapping(0, "DECIMAL");
    assertDecimalMapping(3, "DECIMAL");
    assertDecimalMapping(4, "DECIMAL");
    assertDecimalMapping(5, "DECIMAL");
  }

  @Test
  public void shouldMapDataTypes() {
    SchemaBuilder arrayBuilder = SchemaBuilder.array(
        SchemaBuilder.STRING_SCHEMA
    );

    verifyDataTypeMapping("SMALLINT", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("SMALLINT", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("INT", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("BIGINT", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("REAL", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("DOUBLE PRECISION", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("BOOLEAN", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("TEXT", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("BYTEA", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("DECIMAL", Decimal.schema(0));
    verifyDataTypeMapping("DATE", Date.SCHEMA);
    verifyDataTypeMapping("TIME", Time.SCHEMA);
    verifyDataTypeMapping("TIMESTAMP", Timestamp.SCHEMA);
    verifyDataTypeMapping("TEXT[]", arrayBuilder.build());
  }

  @Test
  public void shouldMapDateSchemaTypeToDateSqlType() {
    assertDateMapping("DATE");
  }

  @Test
  public void shouldMapTimeSchemaTypeToTimeSqlType() {
    assertTimeMapping("TIME");
  }

  @Test
  public void shouldMapTimestampSchemaTypeToTimestampSqlType() {
    assertTimestampMapping("TIMESTAMP");
  }

  @Test
  public void shouldBuildCreateQueryStatement() {
    assertEquals(
        "CREATE TABLE \"myTable\" (\n"
        + "\"c1\" INT NOT NULL,\n"
        + "\"c2\" BIGINT NOT NULL,\n"
        + "\"c3\" TEXT NOT NULL,\n"
        + "\"c4\" TEXT NULL,\n"
        + "\"c5\" DATE DEFAULT '2001-03-15',\n"
        + "\"c6\" TIME DEFAULT '00:00:00.000',\n"
        + "\"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n"
        + "\"c8\" DECIMAL NULL,\n"
        + "PRIMARY KEY(\"c1\"))",
        dialect.buildCreateTableStatement(tableId, sinkRecordFields)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    assertEquals(
        "CREATE TABLE myTable (\n"
        + "c1 INT NOT NULL,\n"
        + "c2 BIGINT NOT NULL,\n"
        + "c3 TEXT NOT NULL,\n"
        + "c4 TEXT NULL,\n"
        + "c5 DATE DEFAULT '2001-03-15',\n"
        + "c6 TIME DEFAULT '00:00:00.000',\n"
        + "c7 TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n"
        + "c8 DECIMAL NULL,\n"
        + "PRIMARY KEY(c1))",
        dialect.buildCreateTableStatement(tableId, sinkRecordFields)
    );
  }

  @Test
  public void shouldBuildAlterTableStatement() {
    assertEquals(
        Arrays.asList(
            "ALTER TABLE \"myTable\" \n"
            + "ADD \"c1\" INT NOT NULL,\n"
            + "ADD \"c2\" BIGINT NOT NULL,\n"
            + "ADD \"c3\" TEXT NOT NULL,\n"
            + "ADD \"c4\" TEXT NULL,\n"
            + "ADD \"c5\" DATE DEFAULT '2001-03-15',\n"
            + "ADD \"c6\" TIME DEFAULT '00:00:00.000',\n"
            + "ADD \"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n"
            + "ADD \"c8\" DECIMAL NULL"
        ),
        dialect.buildAlterTable(tableId, sinkRecordFields)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    assertEquals(
        Arrays.asList(
            "ALTER TABLE myTable \n"
            + "ADD c1 INT NOT NULL,\n"
            + "ADD c2 BIGINT NOT NULL,\n"
            + "ADD c3 TEXT NOT NULL,\n"
            + "ADD c4 TEXT NULL,\n"
            + "ADD c5 DATE DEFAULT '2001-03-15',\n"
            + "ADD c6 TIME DEFAULT '00:00:00.000',\n"
            + "ADD c7 TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n"
            + "ADD c8 DECIMAL NULL"
        ),
        dialect.buildAlterTable(tableId, sinkRecordFields)
    );
  }

  @Test
  public void shouldBuildUpsertStatement() {
    assertEquals(
        "INSERT INTO \"myTable\" (\"id1\",\"id2\",\"columnA\",\"columnB\"," +
        "\"columnC\",\"columnD\") VALUES (?,?,?,?,?,?) ON CONFLICT (\"id1\"," +
        "\"id2\") DO UPDATE SET \"columnA\"=EXCLUDED" +
        ".\"columnA\",\"columnB\"=EXCLUDED.\"columnB\",\"columnC\"=EXCLUDED" +
        ".\"columnC\",\"columnD\"=EXCLUDED.\"columnD\"",
        dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    assertEquals(
        "INSERT INTO myTable (id1,id2,columnA,columnB," +
        "columnC,columnD) VALUES (?,?,?,?,?,?) ON CONFLICT (id1," +
        "id2) DO UPDATE SET columnA=EXCLUDED" +
        ".columnA,columnB=EXCLUDED.columnB,columnC=EXCLUDED" +
        ".columnC,columnD=EXCLUDED.columnD",
        dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD)
    );
  }

  @Test
  public void createOneColNoPk() {
    verifyCreateOneColNoPk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"col1\" INT NOT NULL)");
  }

  @Test
  public void createOneColOnePk() {
    verifyCreateOneColOnePk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" INT NOT NULL," +
        System.lineSeparator() + "PRIMARY KEY(\"pk1\"))");
  }

  @Test
  public void createThreeColTwoPk() {
    verifyCreateThreeColTwoPk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" INT NOT NULL," +
        System.lineSeparator() + "\"pk2\" INT NOT NULL," + System.lineSeparator() +
        "\"col1\" INT NOT NULL," + System.lineSeparator() + "PRIMARY KEY(\"pk1\",\"pk2\"))");

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    verifyCreateThreeColTwoPk(
        "CREATE TABLE myTable (" + System.lineSeparator() + "pk1 INT NOT NULL," +
        System.lineSeparator() + "pk2 INT NOT NULL," + System.lineSeparator() +
        "col1 INT NOT NULL," + System.lineSeparator() + "PRIMARY KEY(pk1,pk2))");
  }

  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol("ALTER TABLE \"myTable\" ADD \"newcol1\" INT NULL");
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols(
        "ALTER TABLE \"myTable\" " + System.lineSeparator() + "ADD \"newcol1\" INT NULL," +
        System.lineSeparator() + "ADD \"newcol2\" INT DEFAULT 42");
  }

  @Test
  public void upsert() {
    TableId customer = tableId("Customer");
    assertEquals(
        "INSERT INTO \"Customer\" (\"id\",\"name\",\"salary\",\"address\") " +
         "VALUES (?,?,?,?) ON CONFLICT (\"id\") DO UPDATE SET \"name\"=EXCLUDED.\"name\"," +
         "\"salary\"=EXCLUDED.\"salary\",\"address\"=EXCLUDED.\"address\"",
        dialect.buildUpsertQueryStatement(
            customer,
            columns(customer, "id"),
            columns(customer, "name", "salary", "address")
        )
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    assertEquals(
        "INSERT INTO Customer (id,name,salary,address) " +
        "VALUES (?,?,?,?) ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name," +
        "salary=EXCLUDED.salary,address=EXCLUDED.address",
        dialect.buildUpsertQueryStatement(
            customer,
            columns(customer, "id"),
            columns(customer, "name", "salary", "address")
        )
    );
  }

  @Test
  public void shouldSanitizeUrlWithoutCredentialsInProperties() {
    assertSanitizedUrl(
        "jdbc:postgresql://localhost/test?user=fred&ssl=true",
        "jdbc:postgresql://localhost/test?user=fred&ssl=true"
    );
  }

  @Test
  public void shouldSanitizeUrlWithCredentialsInUrlProperties() {
    assertSanitizedUrl(
        "jdbc:postgresql://localhost/test?user=fred&password=secret&ssl=true",
        "jdbc:postgresql://localhost/test?user=fred&password=****&ssl=true"
    );
  }

  @Test
  public void addFieldToSchema() {
    verifyFieldSchema(Schema.Type.STRING, Types.OTHER, "JSON");
    verifyFieldSchema(Schema.Type.STRING, Types.OTHER, "JSONB");
    verifyFieldSchema(Schema.Type.STRING, Types.OTHER, "UUID");
    verifyFieldSchema(Schema.Type.ARRAY, Types.ARRAY, "ARRAY");
  }

  @Test
  public void convertSupportedArrayValue() throws Exception {
    Object[] stringArray = new Object[]{"1", "2", "3"};
    Object[] stringArrayWithNulls = new Object[]{"1", null, "3"};

    final String jsonString = "{\"bar\":\"baz\",\"balance\":7.77,\"active\":false}";
    Object[] jsonArray = new Object[]{getPgo("JSON", jsonString), getPgo("JSON", jsonString)};

    final String uuidString = "123e4567-e89b-12d3-a456-426655440000";
    Object[] uuidArray = new Object[]{UUID.fromString(uuidString), UUID.fromString(uuidString)};

    verifyArrayConversion(Arrays.asList("1", "2", "3"), "STRING", stringArray);
    verifyArrayConversion(Arrays.asList("1", null, "3"), "STRING", stringArrayWithNulls);
    verifyArrayConversion(Arrays.asList(uuidString, uuidString), "UUID", uuidArray);

    Array nullArray = null;
    verifyArrayConversion(null, "STRING", nullArray);
  }

  @Test(expected = IOException.class)
  public void convertUnsupportedArrayValue() throws Exception {
    Object[] intArray = new Object[]{1, 2, 3};
    verifyArrayConversion(Arrays.asList(1, 2, 3), "SMALLINT", intArray);
  }

  private void verifyFieldSchema(Schema.Type expected, int sqlType, String typeName) {
    PostgreSqlDatabaseDialect dialect = createDialect();
    ColumnDefinition defn = new ColumnDefinition(
        new ColumnId(new TableId(null, "APP", "test"), "id"),
        sqlType,
        typeName,
        null,
        null,
        null,
        0,
        0,
        false,
        0,
        false,
        false,
        false,
        false,
        false
    );
    SchemaBuilder builder = SchemaBuilder.struct();
    dialect.addFieldToSchema(defn, builder);
    Schema schema = builder.build();
    assertEquals(1, schema.fields().size());
    assertEquals(expected, schema.fields().get(0).schema().type());
  }

  private void verifyArrayConversion(
      Object expectedValue, String typeName, Object[] columnValue
  ) throws Exception {
    // Create a fake connection so that we can create an Array
    MockConnection conn = new MockConnection();

    verifyArrayConversion(expectedValue, typeName, conn.createArrayOf(typeName, columnValue));
  }

  private void verifyArrayConversion(
      Object expectedValue, String typeName, Array columnValue
  ) throws Exception {
    ColumnDefinition columnDefn = mock(ColumnDefinition.class);
    when(columnDefn.id()).thenReturn(new ColumnId(new TableId(null, "APP", "test"), "id"));
    when(columnDefn.precision()).thenReturn(0);
    when(columnDefn.scale()).thenReturn(0);
    when(columnDefn.isSignedNumber()).thenReturn(false);
    when(columnDefn.isOptional()).thenReturn(true);
    when(columnDefn.type()).thenReturn(Types.ARRAY);
    when(columnDefn.typeName()).thenReturn("ARRAY");

    PostgreSqlDatabaseDialect dialect = createDialect();
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();

    // Check the schema field is created with the right type
    dialect.addFieldToSchema(columnDefn, schemaBuilder);
    Schema schema = schemaBuilder.build();
    List<Field> fields = schema.fields();
    assertEquals(1, fields.size());
    Field field = fields.get(0);
    assertEquals(Schema.Type.ARRAY, field.schema().type());

    // Set up the ResultSet
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getArray(1)).thenReturn(columnValue);

    // Check the converter operates correctly
    ColumnMapping mapping = new ColumnMapping(columnDefn, 1, field);
    DatabaseDialect.ColumnConverter converter = dialect.createColumnConverter(mapping);
    assertNotEquals(null, converter);
    Object value = converter.convert(resultSet);
    assertEquals(expectedValue, value);
  }

  private PGobject getPgo(String type, String value) throws Exception {
      PGobject pgo = new PGobject();
      pgo.setType(type);
      pgo.setValue(value);
      return pgo;
  }
}
