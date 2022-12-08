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
import org.apache.commons.lang.StringUtils;

import java.util.Collection;
import java.util.Collections;

public class ConnectionConfig {

    private final String clusterId;
    private final Collection<String> neptuneEndpoints;
    private final int neptunePort;
    private final boolean useIamAuth;
    private boolean useSsl;
    private final ProxyConfig proxyConfig;

    public ConnectionConfig(String clusterId,
                            Collection<String> neptuneEndpoints,
                            int neptunePort,
                            boolean useIamAuth, boolean useSsl, ProxyConfig proxyConfig) {
        this.clusterId = clusterId;
        this.neptuneEndpoints = neptuneEndpoints;
        this.neptunePort = neptunePort;
        this.useIamAuth = useIamAuth;
        this.useSsl = useSsl;
        this.proxyConfig = proxyConfig;
    }

    public Collection<String> endpoints() {
        if (isDirectConnection()) {
            return neptuneEndpoints;
        } else {
            return Collections.singletonList(proxyConfig.endpoint());
        }
    }

    public int port() {
        if (isDirectConnection()) {
            return neptunePort;
        } else {
            return proxyConfig.port();
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
        } else {
            return new HandshakeRequestConfig(neptuneEndpoints, neptunePort, proxyConfig.removeHostHeader());
        }
    }

    public boolean isDirectConnection() {
        return proxyConfig == null;
    }

    public ProxyConfig proxyConfig() {
        return proxyConfig;
    }
}
