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

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

public class EndpointValidatorTest {
    @Test
    public void shouldRemoveProtocol() {
        Collection<String> endpoints = Arrays.asList("my-endpoint", "ws://my-endpoint", "wss://my-endpoint", "http://my-endpoint", "https://my-endpoint");
        for (String endpoint : endpoints) {
            assertEquals("my-endpoint", EndpointValidator.validate(endpoint));
        }
    }

    @Test
    public void shouldRemovePort() {
        Collection<String> endpoints = Arrays.asList("my-endpoint", "my-endpoint:8182");
        for (String endpoint : endpoints) {
            assertEquals("my-endpoint", EndpointValidator.validate(endpoint));
        }
    }

    @Test
    public void shouldRemoveProtocolAndPort() {
        Collection<String> endpoints = Arrays.asList("my-endpoint", "https://my-endpoint:8182");
        for (String endpoint : endpoints) {
            assertEquals("my-endpoint", EndpointValidator.validate(endpoint));
        }
    }
}