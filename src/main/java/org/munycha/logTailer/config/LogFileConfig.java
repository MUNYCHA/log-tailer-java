package org.munycha.logTailer.config;

public class LogFileConfig {

    private String path;
    private String topic;

    public LogFileConfig() {}

    public LogFileConfig(String path, String topic) {
        this.path = path;
        this.topic = topic;
    }

    public String getPath() {
        return path;
    }

    public String getTopic() {
        return topic;
    }
}
