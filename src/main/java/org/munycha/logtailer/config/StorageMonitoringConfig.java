package org.munycha.logtailer.config;

import java.util.List;

public class StorageMonitoringConfig {
    private boolean enabled;
    private String topic;
    private List<String> paths;
    private int intervalHours;

    public StorageMonitoringConfig() {
    }

    public StorageMonitoringConfig(boolean enabled, String topic, List<String> paths, int intervalHours) {
        this.enabled = enabled;
        this.topic = topic;
        this.paths = paths;
        this.intervalHours = intervalHours;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public List<String> getPaths() {
        return paths;
    }

    public void setPaths(List<String> paths) {
        this.paths = paths;
    }

    public int getIntervalHours() {
        return intervalHours;
    }

    public void setIntervalHours(int intervalHours) {
        this.intervalHours = intervalHours;
    }
}
