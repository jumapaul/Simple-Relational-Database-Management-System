package com.simple_rdms.storage_engine.database_manager;

import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Service
public class DatabaseManager {
    private static final String DATA_DIR = "data";
    private String currentDatabase;

    public void createDatabase(String name) throws IOException {
        Path dbPath = Paths.get(DATA_DIR, name);

        if (Files.exists(dbPath)) {
            throw new RuntimeException("Database already exists: " + name);
        }

        Files.createDirectories(dbPath);
    }

    public void useDatabase(String name) {
        Path dbPath = Paths.get(DATA_DIR, name);

        if (!Files.exists(dbPath)) {
            throw new RuntimeException("Database does not exists: + " + name);
        }

        this.currentDatabase = name;
    }

    public String getCurrentDatabase() {
        if (currentDatabase == null) {
            throw new RuntimeException("No database selected");
        }

        return currentDatabase;
    }

    public Path getDatabasePath() {

        return Paths.get(DATA_DIR, getCurrentDatabase());
    }
}
