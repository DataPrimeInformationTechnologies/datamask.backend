package com.data.tools.api.service;

import com.data.tools.api.constants.SQLConst;
import com.data.tools.api.dto.MigrationConfig;
import com.data.tools.api.entity.DbConfiguration;
import com.data.tools.api.entity.DbMigration;
import com.data.tools.api.exceptions.Exceptions;
import com.data.tools.api.exceptions.GlobalException;
import com.data.tools.api.repository.DbConfigurationRepository;
import com.data.tools.api.repository.DbMigrationRepository;
import com.data.tools.api.util.UserHelper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.sql.*;

@Service
@RequiredArgsConstructor
public class DbMigrationService {

    private final DbMigrationRepository dbMigrationRepository;
    private final DbConfigurationRepository dbConfigurationRepository;

    private final DbConnectionService dbConnectionService;

    public void setConfig(MigrationConfig config) {
        DbConfiguration source = this.dbConfigurationRepository.findById(config.sourceId()).orElseThrow();
        DbConfiguration target = this.dbConfigurationRepository.findById(config.targetId()).orElseThrow();
        DbMigration dbMigration;
        if (config.id() != null) {
            dbMigration = this.dbMigrationRepository.findById(config.id())
                   .orElseThrow(() -> GlobalException.throwEx(Exceptions.OBJECT_NOT_FOUND_BY_ID, "DBMigration not found by id"));
        } else {
        dbMigration = new DbMigration();
        }
        dbMigration.setConfigToEntity(config.name(), config.desc(), source, target);
        this.dbMigrationRepository.save(dbMigration);
    }


    public String migrate(Long migrationId,String schemaName,String tableName) throws SQLException {
        try {
            DbMigration dbMigration = this.dbMigrationRepository.findById(migrationId)
                    .orElseThrow(() -> GlobalException.throwEx(Exceptions.OBJECT_NOT_FOUND_BY_ID,"DbMigration not found by id"));

            Connection sourceConnection = dbConnectionService.getConnectionById(dbMigration.getSource().getId());
            Statement sourceStatement = sourceConnection.createStatement();
            ResultSet resultSet = sourceStatement.executeQuery(SQLConst.getTableDDLfromSourceDb(schemaName,tableName));
            String ddlResult = "";
            while(resultSet.next())
            {
                ddlResult = resultSet.getString(1);
            }
            resultSet.close();
            sourceStatement.close();
            sourceConnection.close();

            Connection targetConnection = dbConnectionService.getConnectionById(dbMigration.getTarget().getId());
            Statement targetStatement = targetConnection.createStatement();
            targetStatement.executeQuery(ddlResult);
            targetStatement.close();
            targetConnection.close();
            return "Migration Complete";
        } catch (SQLException e) {
            e.printStackTrace();
            return "Migration failed: " + e.getMessage();
        }
    }


    private DbMigration getDbMigrationByUser() {
        return this.dbMigrationRepository.findByUserAndSelectedTrue(UserHelper.getUser())
                .orElseThrow(() -> GlobalException.throwEx(Exceptions.OBJECT_NOT_FOUND, "DBMigration not found"));

    }


}
