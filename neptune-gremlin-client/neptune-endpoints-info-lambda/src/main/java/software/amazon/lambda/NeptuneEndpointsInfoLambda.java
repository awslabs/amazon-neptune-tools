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

package software.amazon.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import software.amazon.neptune.cluster.*;
import software.amazon.utils.EnvironmentVariableUtils;

import java.io.*;
import java.util.Collection;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;

public class NeptuneEndpointsInfoLambda implements RequestStreamHandler {

    private final ClusterEndpointsRefreshAgent refreshAgent;
    private final AtomicReference<NeptuneClusterMetadata> neptuneClusterMetadata = new AtomicReference<>();
    private final String unavailable;

    public NeptuneEndpointsInfoLambda() {
        this(
                EnvironmentVariableUtils.getMandatoryEnv("clusterId"),
                Integer.parseInt(EnvironmentVariableUtils.getOptionalEnv("pollingIntervalSeconds", "15")),
                EnvironmentVariableUtils.getOptionalEnv("unavailable", "none")
        );
    }

    public NeptuneEndpointsInfoLambda(String clusterId, int pollingIntervalSeconds, String unavailable) {
        refreshAgent = new ClusterEndpointsRefreshAgent(clusterId, EndpointsType.All);
        neptuneClusterMetadata.set(refreshAgent.getClusterMetadata());
        this.unavailable = unavailable.toLowerCase();

        System.out.println(String.format("clusterId: %s", clusterId));
        System.out.println(String.format("pollingIntervalSeconds: %s", pollingIntervalSeconds));
        System.out.println(String.format("unavailable: %s", this.unavailable));

        refreshAgent.startPollingNeptuneAPI(
                (OnNewClusterMetadata) metadata -> neptuneClusterMetadata.set(metadata),
                pollingIntervalSeconds,
                TimeUnit.SECONDS);
    }

    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {

        LambdaLogger logger = context.getLogger();

        EndpointsType endpointsType = null;

        Scanner scanner = new Scanner(input);
        if (scanner.hasNext()) {
            String param = scanner.next().replace("\"", "");
            if (!param.isEmpty()) {
                endpointsType = EndpointsType.valueOf(param);
            }
        }

        if (endpointsType != null) {
            returnEndpointList(endpointsType, logger, output);
        } else {
            returnClusterMetadata(logger, output);
        }
    }

    private void returnClusterMetadata(LambdaLogger logger, OutputStream output) throws IOException {

        logger.log("Returning cluster metadata");

        NeptuneClusterMetadata clusterMetadata = filterMetadataForUnavailableEndpoints(neptuneClusterMetadata.get());
        String results = clusterMetadata.toJsonString();

        logger.log("Results: " + results);

        try (Writer writer = new BufferedWriter(new OutputStreamWriter(output, UTF_8))) {
            writer.write(results);
            writer.flush();
        }
    }

    private void returnEndpointList(EndpointsType endpointsType,
                                    LambdaLogger logger,
                                    OutputStream output) throws IOException {

        logger.log("Returning list of endpoints for EndpointsType: " + endpointsType);

        NeptuneClusterMetadata clusterMetadata = filterMetadataForUnavailableEndpoints(neptuneClusterMetadata.get());
        Collection<String> endpoints = endpointsType.getEndpoints(
                clusterMetadata.getClusterEndpoint(),
                clusterMetadata.getReaderEndpoint(),
                clusterMetadata.getInstances());

        String results = String.join(",", endpoints);
        logger.log("Results: " + results);

        try (Writer writer = new BufferedWriter(new OutputStreamWriter(output, UTF_8))) {
            writer.write(results);
            writer.flush();
        }
    }

    private NeptuneClusterMetadata filterMetadataForUnavailableEndpoints(NeptuneClusterMetadata clusterMetadata){
        for (NeptuneInstanceMetadata instance : clusterMetadata.getInstances()) {
            if (instance.isAvailable()){
                if (unavailable.equals("all")){
                    instance.setStatus("unavailable");
                }
                else if (unavailable.equals("writer") && instance.isPrimary()){
                    instance.setStatus("unavailable");
                }
                else if (unavailable.equals("reader") && instance.isReader()){
                    instance.setStatus("unavailable");
                }
            }
        }
        return clusterMetadata;
    }
}
