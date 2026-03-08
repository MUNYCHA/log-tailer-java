package org.munycha.logtailer.config;

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

    public void setPath(String path) {
        this.path = path;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
