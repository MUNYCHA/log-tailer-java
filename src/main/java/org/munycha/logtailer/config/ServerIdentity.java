package org.munycha.logtailer.config;

public class ServerIdentity {
    private String name;
    private String ip;

    public ServerIdentity() {
    }

    public ServerIdentity(String name, String ip) {
        this.name = name;
        this.ip = ip;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }
}
