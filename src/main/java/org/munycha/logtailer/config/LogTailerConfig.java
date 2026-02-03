package org.munycha.logtailer.config;

import java.util.List;

public class LogTailerConfig {

    private boolean enabled;
    private List<LogFileConfig> files;

    public LogTailerConfig() {
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public List<LogFileConfig> getFiles() {
        return files;
    }

    public void setFiles(List<LogFileConfig> files) {
        this.files = files;
    }
}
