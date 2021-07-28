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

import com.amazonaws.services.neptune.AmazonNeptune;

import java.util.function.Supplier;

public class DoNotCloneCluster implements CloneClusterStrategy {

    private final Supplier<AmazonNeptune> clientSupplier;

    public DoNotCloneCluster(Supplier<AmazonNeptune> clientSupplier) {
        this.clientSupplier = clientSupplier;
    }

    @Override
    public Cluster cloneCluster(ConnectionConfig connectionConfig, ConcurrencyConfig concurrencyConfig) throws Exception {
        return new Cluster() {
            @Override
            public ConnectionConfig connectionConfig() {
                return connectionConfig;
            }

            @Override
            public ConcurrencyConfig concurrencyConfig() {
                return concurrencyConfig;
            }

            @Override
            public Supplier<AmazonNeptune> clientSupplier() {
                return clientSupplier;
            }

            @Override
            public void close() throws Exception {
                //Do nothing
            }
        };
    }
}
