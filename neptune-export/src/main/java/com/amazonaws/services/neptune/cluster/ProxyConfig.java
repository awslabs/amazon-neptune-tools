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

public class ProxyConfig
{
    private final String endpoint;
    private final int port;
    private final boolean removeHostHeader;

    public ProxyConfig(String endpoint, int port, boolean removeHostHeader) {
        this.endpoint = endpoint;
        this.port = port;
        this.removeHostHeader = removeHostHeader;
    }

    public String endpoint() {
        return endpoint;
    }

    public int port() {
        return port;
    }

    public boolean removeHostHeader() {
        return removeHostHeader;
    }

    @Override
    public String toString() {
        return "ProxyConfig{" +
                "endpoint='" + endpoint + '\'' +
                ", port=" + port +
                ", removeHostHeader=" + removeHostHeader +
                '}';
    }
}
