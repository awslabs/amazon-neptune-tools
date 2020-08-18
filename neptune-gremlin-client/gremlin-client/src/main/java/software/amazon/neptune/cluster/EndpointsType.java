/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package software.amazon.neptune.cluster;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public enum EndpointsType implements EndpointsSelector {
    All {
        @Override
        public Collection<String> getEndpoints(String clusterEndpoint,
                                               String readerEndpoint,
                                               Collection<NeptuneInstanceProperties> instances) {
            return instances.stream()
                    .filter(NeptuneInstanceProperties::isAvailable)
                    .map(NeptuneInstanceProperties::getEndpoint)
                    .collect(Collectors.toList());
        }
    },
    Primary {
        @Override
        public Collection<String> getEndpoints(String clusterEndpoint,
                                               String readerEndpoint,
                                               Collection<NeptuneInstanceProperties> instances) {
            List<String> results = instances.stream()
                    .filter(NeptuneInstanceProperties::isPrimary)
                    .filter(NeptuneInstanceProperties::isAvailable)
                    .map(NeptuneInstanceProperties::getEndpoint)
                    .collect(Collectors.toList());

            if (results.isEmpty()){
                return ClusterEndpoint.getEndpoints(clusterEndpoint, readerEndpoint, instances);
            }

            return results;
        }
    },
    ReadReplicas {
        @Override
        public Collection<String> getEndpoints(String clusterEndpoint,
                                               String readerEndpoint,
                                               Collection<NeptuneInstanceProperties> instances) {

            List<String> results = instances.stream()
                    .filter(NeptuneInstanceProperties::isReader)
                    .filter(NeptuneInstanceProperties::isAvailable)
                    .map(NeptuneInstanceProperties::getEndpoint)
                    .collect(Collectors.toList());

            if (results.isEmpty()) {
                return ReaderEndpoint.getEndpoints(clusterEndpoint, readerEndpoint, instances);
            }

            return results;
        }
    },
    ClusterEndpoint {
        @Override
        public Collection<String> getEndpoints(String clusterEndpoint,
                                               String readerEndpoint,
                                               Collection<NeptuneInstanceProperties> instances) {

            return Collections.singletonList(clusterEndpoint);
        }
    },
    ReaderEndpoint {
        @Override
        public Collection<String> getEndpoints(String clusterEndpoint,
                                               String readerEndpoint,
                                               Collection<NeptuneInstanceProperties> instances) {

            return Collections.singletonList(readerEndpoint);
        }
    }

}
