package org.munycha.logtailer.config;

public class AppConfig {

    private String bootstrapServers;
    private IdentityConfig identity;
    private LogTailerConfig logTailer;
    private StorageMonitoringConfig storageMonitoring;

    public AppConfig() {
    }

    public AppConfig(String bootstrapServers, IdentityConfig identity, LogTailerConfig logTailer, StorageMonitoringConfig storageMonitoring) {
        this.bootstrapServers = bootstrapServers;
        this.identity = identity;
        this.logTailer = logTailer;
        this.storageMonitoring = storageMonitoring;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public IdentityConfig getIdentity() {
        return identity;
    }

    public void setIdentity(IdentityConfig identity) {
        this.identity = identity;
    }

    public LogTailerConfig getLogTailer() {
        return logTailer;
    }

    public void setLogTailer(LogTailerConfig logTailer) {
        this.logTailer = logTailer;
    }

    public StorageMonitoringConfig getStorageMonitoring() {
        return storageMonitoring;
    }

    public void setStorageMonitoring(StorageMonitoringConfig storageMonitoring) {
        this.storageMonitoring = storageMonitoring;
    }

    public void validate() {
        require(bootstrapServers != null && !bootstrapServers.trim().isEmpty(),
                "'bootstrapServers' is required");

        require(identity != null, "'identity' is required");
        require(identity.getSystem() != null, "'identity.system' is required");
        require(identity.getSystem().getId() != null && !identity.getSystem().getId().trim().isEmpty(),
                "'identity.system.id' is required");
        require(identity.getSystem().getName() != null && !identity.getSystem().getName().trim().isEmpty(),
                "'identity.system.name' is required");
        require(identity.getServer() != null, "'identity.server' is required");
        require(identity.getServer().getName() != null && !identity.getServer().getName().trim().isEmpty(),
                "'identity.server.name' is required");

        if (logTailer != null && logTailer.isEnabled()) {
            require(logTailer.getFiles() != null && !logTailer.getFiles().isEmpty(),
                    "'logTailer.files' must not be empty when enabled");
            for (LogFileConfig f : logTailer.getFiles()) {
                require(f.getPath() != null && !f.getPath().trim().isEmpty(),
                        "each 'logTailer.files' entry must have a 'path'");
                require(f.getTopic() != null && !f.getTopic().trim().isEmpty(),
                        "each 'logTailer.files' entry must have a 'topic'");
            }
        }

        if (storageMonitoring != null && storageMonitoring.isEnabled()) {
            require(storageMonitoring.getTopic() != null && !storageMonitoring.getTopic().trim().isEmpty(),
                    "'storageMonitoring.topic' is required when enabled");
            require(storageMonitoring.getPaths() != null && !storageMonitoring.getPaths().isEmpty(),
                    "'storageMonitoring.paths' must not be empty when enabled");
            require(storageMonitoring.getIntervalHours() > 0,
                    "'storageMonitoring.intervalHours' must be greater than 0");
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException("Config validation failed: " + message);
        }
    }
}
