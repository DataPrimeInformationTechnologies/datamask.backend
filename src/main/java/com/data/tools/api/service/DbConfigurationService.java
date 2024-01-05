package com.data.tools.api.service;

import com.data.tools.api.entity.DbConfiguration;
import com.data.tools.api.entity.User;
import com.data.tools.api.repository.DbConfigurationRepository;
import com.data.tools.api.repository.UserRepository;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
@Service
@RequiredArgsConstructor
public class DbConfigurationService {

    private final DbConfigurationRepository dbConfigurationRepository;

    private final UserRepository userRepository;

    public List<DbConfiguration> getAllDbConfigurations() {
        return dbConfigurationRepository.findAll();
    }

    public DbConfiguration addConfiguration(DbConfiguration dbConfiguration,Long userId) {
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new EntityNotFoundException("User not found with id:" + userId));
        dbConfiguration.setUser(user);
        return dbConfigurationRepository.save(dbConfiguration);
    }

    public DbConfiguration getDbConfigurationById(Long confId) {
        return dbConfigurationRepository.findById(confId)
                .orElseThrow(() -> new EntityNotFoundException("Db configuration not found with id:"+confId));
    }

    public  void deleteDbConfigurationById(Long confId) {
        dbConfigurationRepository.deleteById(confId);
    }

    public  DbConfiguration updateDbConfiguration(Long confId,DbConfiguration updateConfig,Long userId) {
        DbConfiguration  existingConfig = dbConfigurationRepository.findById(confId)
                .orElseThrow(() -> new EntityNotFoundException("Db configuration not found with id:"+confId));
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new EntityNotFoundException("User not found with id:" + userId));
        existingConfig.setName(updateConfig.getName());
        existingConfig.setDescription(updateConfig.getDescription());
        existingConfig.setUrl(updateConfig.getUrl());
        existingConfig.setUserName(updateConfig.getUserName());
        existingConfig.setPassword(updateConfig.getPassword());
        existingConfig.setUpdatedAt(updateConfig.getUpdatedAt());
        existingConfig.setUser(user);
        return  dbConfigurationRepository.save(existingConfig);
    }
}

























