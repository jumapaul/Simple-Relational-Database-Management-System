package com.simple_rdms;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
public class SimpleRDMSSApplication {
    public static void main(String[] args) throws IOException {
        SpringApplication.run(SimpleRDMSSApplication.class);
        //Add db name;
//        TableSchema schema = new TableSchema(
//                "users",
//                List.of(
//                        new ColumnDef("id", ColumnType.INT),
//                        new ColumnDef("firstname", ColumnType.STRING),
//                        new ColumnDef("lastname", ColumnType.STRING),
//                        new ColumnDef("email", ColumnType.STRING)
//                ),
//                0
//        );
//
//        TableFile userTable = new TableFile(schema);
//
////        userTable.update(1, new RowLayout(
////                schema, 1, "Paul", "Juma", "abc@gmail"
////        ));
//
//        SQLTableInterface sql = new SQLTableInterface(userTable, schema);

//        sql.executeSQL("INSERT INTO users (id, firstname, lastname, email) VALUES (1, 'John', 'Doe', 'john@example.com')");
//        sql.executeSQL("INSERT INTO users (id, firstname, lastname, email) VALUES (2, 'Jambo', 'Doe', 'john@example.com')");
//        sql.executeSQL("INSERT INTO users (id, firstname, lastname, email) VALUES (3, 'Oligo', 'Doe', 'john@example.com')");
//        sql.executeSQL("INSERT INTO users (id, firstname, lastname, email) VALUES (4, 'Kamae', 'Doe', 'john@example.com')");

//        sql.executeSQL("DELETE FROM users WHERE id = 1");

//        sql.executeSQL("SELECT * FROM users");

//        sql.executeSQL(
//                "UPDATE users SET firstname='Oluu', lastname='Juma', email='abc@gmail.com' WHERE id=3"
//        );

//        sql.executeSQL("SELECT * FROM users");
//        System.out.println("------------------");
//        sql.executeSQL("SELECT * FROM users WHERE id = 3");

//        userTable.close();
//        RowLayout row = userTable.findByPrimaryKey(2);
//
//        System.out.println(row);
    }
}