package org.munycha.logTailer.config;

public class IdentityConfig {
    private SystemIdentity system;
    private ServerIdentity server;

    public IdentityConfig() {
    }

    public IdentityConfig(SystemIdentity system, ServerIdentity server) {
        this.system = system;
        this.server = server;
    }

    public SystemIdentity getSystem() {
        return system;
    }

    public void setSystem(SystemIdentity system) {
        this.system = system;
    }

    public ServerIdentity getServer() {
        return server;
    }

    public void setServer(ServerIdentity server) {
        this.server = server;
    }
}
