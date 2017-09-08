package io.confluent.connect.jdbc.source;


import com.mockrunner.mock.jdbc.MockConnection;
import com.mockrunner.mock.jdbc.MockResultSet;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.json.JSONObject;
import org.junit.Test;
import org.postgresql.util.PGobject;

import javax.sql.rowset.RowSetMetaDataImpl;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class DataConverterTest {

    /**
     * Helpers
     */
    private ResultSetMetaData createJSONMetadata() throws SQLException {
        RowSetMetaDataImpl rowSetMetaData = new RowSetMetaDataImpl();
        rowSetMetaData.setColumnCount(1);
        int columnIndex = 1;
        rowSetMetaData.setColumnType(columnIndex, Types.OTHER);
        rowSetMetaData.setColumnTypeName(columnIndex, "JSON");
        rowSetMetaData.setColumnLabel(columnIndex, "json_column");
        return rowSetMetaData;
    }

    private ResultSetMetaData createJSONBMetadata() throws SQLException {
        RowSetMetaDataImpl rowSetMetaData = new RowSetMetaDataImpl();
        rowSetMetaData.setColumnCount(1);
        int columnIndex = 1;
        rowSetMetaData.setColumnType(columnIndex, Types.OTHER);
        rowSetMetaData.setColumnTypeName(columnIndex, "JSONB");
        rowSetMetaData.setColumnLabel(columnIndex, "jsonb_column");
        return rowSetMetaData;
    }

    private ResultSetMetaData createUUIDMetadata() throws SQLException {
        RowSetMetaDataImpl rowSetMetaData = new RowSetMetaDataImpl();
        rowSetMetaData.setColumnCount(1);
        int columnIndex = 1;
        rowSetMetaData.setColumnType(columnIndex, Types.OTHER);
        rowSetMetaData.setColumnTypeName(columnIndex, "UUID");
        rowSetMetaData.setColumnLabel(columnIndex, "uuid_column");
        return rowSetMetaData;
    }

    private ResultSetMetaData createArrayMetadata() throws SQLException {
        RowSetMetaDataImpl rowSetMetaData = new RowSetMetaDataImpl();
        rowSetMetaData.setColumnCount(1);
        int columnIndex = 1;
        rowSetMetaData.setColumnType(columnIndex, Types.ARRAY);
        rowSetMetaData.setColumnTypeName(columnIndex, "ARRAY");
        rowSetMetaData.setColumnLabel(columnIndex, "array_column");
        return rowSetMetaData;
    }

    private PGobject getPgo(String type, String value) throws SQLException {
        PGobject pgo = new PGobject();
        pgo.setType(type);
        pgo.setValue(value);
        return pgo;
    }

    /**
     * Schema conversion tests
     */
    @Test
    public void convertsJSONSchemaToString() throws SQLException {
        boolean mapNumerics = false;
        final String tableName = "test";
        final ResultSetMetaData metaData = createJSONMetadata();
        Schema schema = DataConverter.convertSchema(tableName, metaData, mapNumerics);
        assertEquals(schema.fields().size(), 1);
        assertEquals("Expected JSON to be converted to a string", Schema.Type.STRING, schema.fields().get(0).schema().type());
    }

    @Test
    public void convertsJSONBSchemaToString() throws SQLException {
        boolean mapNumerics = false;
        final String tableName = "test";
        final ResultSetMetaData metaData = createJSONBMetadata();
        Schema schema = DataConverter.convertSchema(tableName, metaData, mapNumerics);
        assertEquals(schema.fields().size(), 1);
        assertEquals("Expected JSONB to be converted to a string", Schema.Type.STRING, schema.fields().get(0).schema().type());
    }

    @Test
    public void convertsUUIDSchemaToString() throws SQLException {
        boolean mapNumerics = false;
        final String tableName = "test";
        final ResultSetMetaData metaData = createUUIDMetadata();
        Schema schema = DataConverter.convertSchema(tableName, metaData, mapNumerics);
        assertEquals(schema.fields().size(), 1);
        assertEquals("Expected UUID to be converted to a string", Schema.Type.STRING, schema.fields().get(0).schema().type());
    }

    @Test
    public void supportsArraySchemaType() throws SQLException {
        boolean mapNumerics = false;
        final String tableName = "test";
        final ResultSetMetaData metaData = createArrayMetadata();
        Schema schema = DataConverter.convertSchema(tableName, metaData, mapNumerics);
        assertEquals(schema.fields().size(), 1);
        assertEquals("Expected Array to be supported", Schema.Type.ARRAY, schema.fields().get(0).schema().type());
    }

    /**
     * Field value conversion tests
     */
    @Test
    public void convertsJSONValueToString() throws SQLException {
        boolean mapNumerics = false;
        final String tableName = "test";
        final String jsonString = "{\"bar\":\"baz\",\"balance\":7.77,\"active\":false}";
        final ResultSetMetaData metaData = createJSONMetadata();
        Schema schema = DataConverter.convertSchema(tableName, metaData, mapNumerics);

        MockResultSet mockResultSet = new MockResultSet("myResults");
        mockResultSet.addColumn("json_column");
        mockResultSet.setResultSetMetaData(metaData);
        mockResultSet.addRow(new Object[] { new JSONObject(jsonString) });

        // Point the cursor at the first row
        mockResultSet.next();

        Struct record = DataConverter.convertRecord(schema, mockResultSet, mapNumerics);

        assertEquals("Expected JSON to match JSON string", jsonString, record.get("json_column"));
    }

    @Test
    public void convertsJSONBValueToString() throws SQLException {
        boolean mapNumerics = false;
        final String tableName = "test";
        final String jsonbString = "{\"bar\":\"baz\",\"balance\":7.77,\"active\":false}";
        final ResultSetMetaData metaData = createJSONBMetadata();
        Schema schema = DataConverter.convertSchema(tableName, metaData, mapNumerics);

        MockResultSet mockResultSet = new MockResultSet("myResults");
        mockResultSet.addColumn("jsonb_column");
        mockResultSet.setResultSetMetaData(metaData);
        mockResultSet.addRow(new Object[] {new JSONObject(jsonbString)});

        // Point the cursor at the first row
        mockResultSet.next();

        Struct record = DataConverter.convertRecord(schema, mockResultSet, mapNumerics);

        assertEquals("Expected JSONB to match JSONB string", jsonbString, record.get("jsonb_column"));
    }

    @Test
    public void convertsUUIDValueToString() throws SQLException {
        boolean mapNumerics = false;
        final String tableName = "test";
        final String uuidString = "123e4567-e89b-12d3-a456-426655440000";
        final ResultSetMetaData metaData = createUUIDMetadata();
        Schema schema = DataConverter.convertSchema(tableName, metaData, mapNumerics);

        MockResultSet mockResultSet = new MockResultSet("myResults");
        mockResultSet.addColumn("uuid_column");
        mockResultSet.setResultSetMetaData(metaData);
        mockResultSet.addRow(new Object[] { UUID.fromString(uuidString) });

        // Point the cursor at the first row
        mockResultSet.next();

        Struct record = DataConverter.convertRecord(schema, mockResultSet, mapNumerics);

        assertEquals("Expected UUID to match UUID string", uuidString, record.get("uuid_column"));
    }

    @Test
    public void supportsArrayValue() throws SQLException {
        // Setup
        boolean mapNumerics = false;
        final String tableName = "test";
        final ResultSetMetaData metaData = createArrayMetadata();
        Schema schema = DataConverter.convertSchema(tableName, metaData, mapNumerics);

        MockResultSet mockResultSet = new MockResultSet("myResults");
        mockResultSet.addColumn("array_column");
        mockResultSet.setResultSetMetaData(metaData);

        // Create a fake connection so that we can create an Array
        Connection con = new MockConnection();
        Object[] testArray = new Object[]{"1", "2", "3"};
        Array numbersArray = con.createArrayOf("STRING", testArray);
        mockResultSet.addRow(new Object[]{numbersArray});

        // Point the cursor at the first row
        mockResultSet.next();

        Struct record = DataConverter.convertRecord(schema, mockResultSet, mapNumerics);
        List<String> expectedResult = Arrays.asList("1", "2", "3");
        assertEquals("Expected Array to match Array of Strings", expectedResult, record.get("array_column"));
    }

    @Test
    public void convertsNonStringArrayValuesToStringArrayValues() throws SQLException {
        // Setup
        boolean mapNumerics = false;
        final String tableName = "test";
        final ResultSetMetaData metaData = createArrayMetadata();
        Schema schema = DataConverter.convertSchema(tableName, metaData, mapNumerics);

        MockResultSet mockResultSet = new MockResultSet("myResults");
        mockResultSet.addColumn("array_column");
        mockResultSet.setResultSetMetaData(metaData);

        // Create a fake connection so that we can create an Array
        Connection con = new MockConnection();
        Object[] testArray = new Object[]{1, 2, 3};
        Array numbersArray = con.createArrayOf("STRING", testArray);
        mockResultSet.addRow(new Object[]{numbersArray});

        // Point the cursor at the first row
        mockResultSet.next();

        // This throws an exception which causes confluent to skip the record and log the exception.
        Struct record = DataConverter.convertRecord(schema, mockResultSet, mapNumerics);
        assertEquals("Expected Array to match Array of Strings", null, record.get("array_column"));
    }

    @Test
    public void convertsJsonArrayValuesToStringArrayValues() throws SQLException {
        // Setup
        boolean mapNumerics = false;
        final String tableName = "test";
        final String jsonString = "{\"bar\":\"baz\",\"balance\":7.77,\"active\":false}";
        final ResultSetMetaData metaData = createArrayMetadata();
        Schema schema = DataConverter.convertSchema(tableName, metaData, mapNumerics);

        MockResultSet mockResultSet = new MockResultSet("myResults");
        mockResultSet.addColumn("array_column");
        mockResultSet.setResultSetMetaData(metaData);

        // Create a fake connection so that we can create an Array
        Connection con = new MockConnection();
        Object[] testArray = new Object[]{getPgo("JSON", jsonString), getPgo("JSON", jsonString)};
        Array numbersArray = con.createArrayOf("JSON", testArray);
        mockResultSet.addRow(new Object[]{numbersArray});

        // Point the cursor at the first row
        mockResultSet.next();

        Struct record = DataConverter.convertRecord(schema, mockResultSet, mapNumerics);
        List<String> expectedResult = Arrays.asList(jsonString, jsonString);
        assertEquals("Expected Array to match Array of Strings", expectedResult, record.get("array_column"));
    }

    @Test
    public void convertsAnArrayWithNullsToStringArrayWithNulls() throws SQLException {
        // Setup
        final String tableName = "test";
        final ResultSetMetaData metaData = createArrayMetadata();
        boolean mapNumerics = false;
        Schema schema = DataConverter.convertSchema(tableName, metaData, mapNumerics);

        MockResultSet mockResultSet = new MockResultSet("myResults");
        mockResultSet.addColumn("array_column");
        mockResultSet.setResultSetMetaData(metaData);

        // Create a fake connection so that we can create an Array
        Connection con = new MockConnection();
        Object[] testArray = new Object[]{"a", null, "b"};
        Array numbersArray = con.createArrayOf("STRING", testArray);
        mockResultSet.addRow(new Object[]{numbersArray});

        // Point the cursor at the first row
        mockResultSet.next();

        Struct record = DataConverter.convertRecord(schema, mockResultSet, mapNumerics);
        List<String> expectedResult = Arrays.asList("a", null, "b");
        assertEquals("Expected Array to match Array of Strings and nulls", expectedResult, record.get("array_column"));

    }
}
