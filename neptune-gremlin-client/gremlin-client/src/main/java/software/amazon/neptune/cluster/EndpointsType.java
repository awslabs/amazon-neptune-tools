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
import java.util.Map;
import java.util.stream.Collectors;

public enum EndpointsType implements EndpointsSelector {
    All {
        @Override
        public Collection<String> getEndpoints(String primaryId,
                                               Collection<String> replicaIds,
                                               Map<String, NeptuneInstanceProperties> instances) {
            return instances.values().stream()
                    .filter(NeptuneInstanceProperties::isAvailable)
                    .map(NeptuneInstanceProperties::getEndpoint)
                    .collect(Collectors.toList());
        }
    },
    Primary {
        @Override
        public Collection<String> getEndpoints(String primaryId,
                                               Collection<String> replicaIds,
                                               Map<String, NeptuneInstanceProperties> instances) {
            return instances.values().stream()
                    .filter(i -> primaryId.equals(i.getInstanceId()))
                    .filter(NeptuneInstanceProperties::isAvailable)
                    .map(NeptuneInstanceProperties::getEndpoint)
                    .collect(Collectors.toList());
        }
    },
    ReadReplicas {
        @Override
        public Collection<String> getEndpoints(String primaryId,
                                               Collection<String> replicaIds,
                                               Map<String, NeptuneInstanceProperties> instances) {

            if (replicaIds.isEmpty()) {
                return Collections.singleton(instances.get(primaryId).getEndpoint());
            }

            return instances.values().stream()
                    .filter(i -> replicaIds.contains(i.getInstanceId()))
                    .filter(NeptuneInstanceProperties::isAvailable)
                    .map(NeptuneInstanceProperties::getEndpoint)
                    .collect(Collectors.toList());
        }
    };
}
