package com.amazonaws.services.neptune.auth;

import java.util.Collection;
import java.util.Collections;

public class ConnectionConfig {

    private final Collection<String> neptuneEndpoints;
    private final int neptunePort;
    private final String nlbEndpoint;
    private final String albEndpoint;
    private final int lbPort;
    private final boolean useIamAuth;

    public ConnectionConfig(Collection<String> neptuneEndpoints,
                            int neptunePort,
                            String nlbEndpoint,
                            String albEndpoint,
                            int lbPort,
                            boolean useIamAuth) {
        this.neptuneEndpoints = neptuneEndpoints;
        this.neptunePort = neptunePort;
        this.nlbEndpoint = nlbEndpoint;
        this.albEndpoint = albEndpoint;
        this.lbPort = lbPort;
        this.useIamAuth = useIamAuth;
    }

    public Collection<String> endpoints() {
        if (isDirectConnection()) {
            return neptuneEndpoints;
        } else if (nlbEndpoint != null) {
            return Collections.singletonList(nlbEndpoint);
        } else {
            return Collections.singletonList(albEndpoint);
        }
    }

    public int port() {
        if (isDirectConnection()) {
            return neptunePort;
        } else {
            return lbPort;
        }
    }

    public boolean useIamAuth() {
        return useIamAuth;
    }

    public HandshakeRequestConfig handshakeRequestConfig() {
        if (isDirectConnection()) {
            return new HandshakeRequestConfig(Collections.emptyList(), neptunePort, false);
        } else if (nlbEndpoint != null) {
            return new HandshakeRequestConfig(neptuneEndpoints, neptunePort, false);
        } else {
            return new HandshakeRequestConfig(neptuneEndpoints, neptunePort, true);
        }
    }

    public boolean isDirectConnection() {
        return nlbEndpoint == null && albEndpoint == null;
    }
}
