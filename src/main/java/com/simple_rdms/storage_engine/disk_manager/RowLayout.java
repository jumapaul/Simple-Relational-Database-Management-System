package com.simple_rdms.storage_engine.disk_manager;

import com.simple_rdms.storage_engine.schema.ColumnDef;
import com.simple_rdms.storage_engine.schema.TableSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Row layout
 */
public class RowLayout {

    private final Object[] values;
    private final TableSchema schema;

    public RowLayout(TableSchema schema, Object... values) {
        this.values = values;
        this.schema = schema;
    }

    //Convert row data to binary format
    public byte[] serialize() {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(); //Growable byte buffer since we do not know the row size in advance.

            List<ColumnDef> columns = schema.getColumns();
            for (int i = 0; i < values.length; i++) {
                ColumnDef column = columns.get(i); //Each value is serialized according to column type
                switch (column.getType()) {
                    case INT ->
                            outputStream.write(ByteBuffer.allocate(4).putInt((Integer) values[i]).array()); //int is 4 bytes
                    case STRING -> {
                        byte[] stringByte = ((String) values[i]).getBytes(StandardCharsets.UTF_8);

                        //[4-byte length][actual string bytes]
                        //Strings are variable length, so we have to get length to know where the next column begins
                        outputStream.write(ByteBuffer.allocate(4).putInt(stringByte.length).array());
                        outputStream.write(stringByte);
                    }
                    case BOOLEAN -> outputStream.write((Boolean) values[i] ? 1 : 0); //0 and 1
                    case FLOAT -> outputStream.write(ByteBuffer.allocate(4).putFloat((Float) values[i]).array());
                    case DOUBLE ->
                            outputStream.write(ByteBuffer.allocate(8).putDouble((Double) values[i]).array()); //*bytes because of high precision
                }
            }

            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /*
     * Bytes to java objects
     *
     */
    public static RowLayout deserialize(ByteBuffer buffer, TableSchema schema) {
        Object[] values = new Object[schema.getColumns().size()]; //Value container for the columns we have

        //Loop over the columns to decode
        for (int i = 0; i < values.length; i++) {
            ColumnDef column = schema.getColumns().get(i);

            switch (column.getType()) {
                case INT -> values[i] = buffer.getInt();
                case STRING -> {
                    int len = buffer.getInt(); //Read 4 byte length
                    byte[] stringBytes = new byte[len]; //Allocate exact byte array.
                    buffer.get(stringBytes);
                    values[i] = new String(stringBytes, StandardCharsets.UTF_8); //Decode utf-8
                }
                case BOOLEAN -> values[i] = buffer.get() == 1; //Reads 1byte
                case FLOAT -> values[i] = buffer.getFloat(); //Read 4
                case DOUBLE -> values[i] = buffer.getDouble(); //Read 8
            }
        }

        return new RowLayout(schema, values);
    }

    public Object getValues(int columnIndex) {
        return values[columnIndex];
    }

    public Object getPrimaryKey() {
        return values[schema.getPrimaryKeyIndex()];
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Object val : values) {
            stringBuilder.append(val).append(", ");
        }

        if (stringBuilder.length() > 2) stringBuilder.setLength(stringBuilder.length() - 2);
        return stringBuilder.toString();
    }
}
