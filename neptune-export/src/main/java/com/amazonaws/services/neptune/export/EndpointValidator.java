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

package com.amazonaws.services.neptune.export;

import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;

public class EndpointValidator {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(EndpointValidator.class);

    public static Collection<String> validate(Collection<String> endpoints) {
        Collection<String> validatedEndpoints = new ArrayList<>();
        for (String endpoint : endpoints) {
            validatedEndpoints.add(validate(endpoint));
        }
        return validatedEndpoints;
    }

    public static String validate(String endpoint) {
        if (endpoint.startsWith("ws://") || endpoint.startsWith("wss://") || endpoint.startsWith("http://") || endpoint.startsWith("https://")) {
            logger.warn("Endpoint cannot contain protocol. Removing protocol: {}", endpoint);
            endpoint = endpoint.substring(endpoint.indexOf("//") + 2);
        }
        if (endpoint.contains(":")) {
            logger.warn("Endpoint cannot contain port. Removing port: {}", endpoint);
            endpoint = endpoint.substring(0, endpoint.indexOf(":"));
        }
        return endpoint;
    }

}
