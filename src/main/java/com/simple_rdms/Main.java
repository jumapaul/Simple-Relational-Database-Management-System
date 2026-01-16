package com.simple_rdms;

import com.simple_rdms.storage_engine.disk_manager.TableFile;
import com.simple_rdms.storage_engine.page.RowLayout;
import com.simple_rdms.storage_engine.schema.ColumnDef;
import com.simple_rdms.storage_engine.schema.ColumnType;
import com.simple_rdms.storage_engine.schema.TableSchema;

import java.io.IOException;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException {
        //Add db name;
        TableSchema schema = new TableSchema(
                "users",
                List.of(
                        new ColumnDef("id", ColumnType.INT),
                        new ColumnDef("firstname", ColumnType.STRING),
                        new ColumnDef("lastname", ColumnType.STRING),
                        new ColumnDef("email", ColumnType.STRING)
                ),
                0
        );

        TableFile userTable = new TableFile(schema);

//        userTable.insert(new RowLayout(schema, 1, "Jame", "Ambole", "abc@gmail.com"));
//        userTable.insert(new RowLayout(schema, 2, "Jame", "Ambole", "abc@gmail.com"));
//        userTable.insert(new RowLayout(schema, 3, "Uhuu", "Kesia", "abc@gmail.com"));
//        userTable.insert(new RowLayout(schema, 4, "Paul", "Juma", "abc@gmail.com"));
//        userTable.insert(new RowLayout(schema, 5, "Paul", "Juma", "abc@gmail.com"));


        RowLayout rowLayout = userTable.findByPrimaryKey(5);
        System.out.println("The item is: " + rowLayout);

//        boolean delete = userTable.delete(1);
//        System.out.println("Delete state is " + delete);
//
        userTable.update(3, new RowLayout(
                schema, 3, "John", "Kamau", "cd@gmail.com"
        ));

        for (RowLayout row : userTable.readAll()) {
            System.out.println(row);
        }

        userTable.close();
    }
}