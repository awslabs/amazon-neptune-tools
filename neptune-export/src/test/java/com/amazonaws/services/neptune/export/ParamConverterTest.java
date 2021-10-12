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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

public class ParamConverterTest {
    @Test
    public void shouldConvertCamelCaseToDashDelimitedLowerCase(){
        assertEquals("my-long-args", ParamConverter.toCliArg("myLongArgs"));
    }

    @Test
    public void shouldSingularizeValue(){
        assertEquals("endpoint", ParamConverter.singularize("endpoints"));
        assertEquals("name", ParamConverter.singularize("name"));
        assertEquals("query", ParamConverter.singularize("queries"));
    }

    @Test
    public void shouldConvertParams() throws JsonProcessingException {
        String json = "{\n" +
                "    \"endpoints\": [\"endpoint1\", \"endpoint2\"],\n" +
                "    \"profile\": \"neptune_ml\",\n" +
                "    \"useIamAuth\": true,\n" +
                "    \"cloneCluster\": true,\n" +
                "    \"cloneClusterReplicaCount\": 2\n" +
                "  }";

        JsonNode jsonNode = new ObjectMapper().readTree(json);

        Args args = ParamConverter.fromJson("export-pg", jsonNode);

        assertEquals("export-pg --endpoint 'endpoint1' --endpoint 'endpoint2' --profile 'neptune_ml' --use-iam-auth --clone-cluster --clone-cluster-replica-count 2", args.toString());
    }

}