/*
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.cluster;

import com.amazonaws.services.neptune.auth.HandshakeRequestConfig;

import java.util.Collection;
import java.util.Collections;

public class ConnectionConfig {

    private final Collection<String> neptuneEndpoints;
    private final int neptunePort;
    private final String nlbEndpoint;
    private final String albEndpoint;
    private final int lbPort;
    private final boolean useIamAuth;
    private boolean useSsl;

    public ConnectionConfig(Collection<String> neptuneEndpoints,
                            int neptunePort,
                            String nlbEndpoint,
                            String albEndpoint,
                            int lbPort,
                            boolean useIamAuth,
                            boolean useSsl) {
        this.neptuneEndpoints = neptuneEndpoints;
        this.neptunePort = neptunePort;
        this.nlbEndpoint = nlbEndpoint;
        this.albEndpoint = albEndpoint;
        this.lbPort = lbPort;
        this.useIamAuth = useIamAuth;
        this.useSsl = useSsl;
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

    public boolean useSsl() {
        return useSsl;
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

    public String nlbEndpoint() {
        return nlbEndpoint;
    }

    public String albEndpoint() {
        return albEndpoint;
    }

    public int lbPort() {
        return lbPort;
    }
}
