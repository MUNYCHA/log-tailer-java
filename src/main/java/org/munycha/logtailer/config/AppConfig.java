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
}
