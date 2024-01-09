package com.data.tools.api.controller;

import com.data.tools.api.dto.MigrationConfig;
import com.data.tools.api.exceptions.Exceptions;
import com.data.tools.api.exceptions.GlobalException;
import com.data.tools.api.service.DbMigrationService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.repository.query.Param;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;

@RequiredArgsConstructor
@RestController
@RequestMapping("/migrate")
public class DbMigrationController {

    private final DbMigrationService dbMigrationService;

    @PostMapping("/config")
    public ResponseEntity<Boolean> addMigrationConfigs(@RequestBody MigrationConfig config) {
        this.dbMigrationService.setConfig(config);
        return ResponseEntity.ok(true);
    }

    @PutMapping("/config")
    public ResponseEntity<Boolean> setMigrationConfigs(@RequestBody MigrationConfig config) {
        if(config.id() == null) {
            throw GlobalException.throwEx(Exceptions.ID_NOT_FOUND, "MigrationConfig must has an id");
        }
        this.dbMigrationService.setConfig(config);
        return ResponseEntity.ok(true);
    }

    @GetMapping("/table")
    public ResponseEntity<String> migrateTable(@Param("migrationId") Long migrationId,@Param("schemaName") String schemaName,@Param("tableName") String tableName) throws SQLException {
       return ResponseEntity.ok(dbMigrationService.migrate(migrationId,schemaName,tableName));
    }
}

