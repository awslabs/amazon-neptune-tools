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
import software.amazon.neptune.cluster.ClusterEndpointsRefreshAgent;
import software.amazon.neptune.cluster.EndpointsSelector;
import software.amazon.neptune.cluster.EndpointsType;
import software.amazon.utils.EnvironmentVariableUtils;

import java.io.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;

public class NeptuneEndpointsInfoLambda implements RequestStreamHandler {

    private final ClusterEndpointsRefreshAgent refreshAgent;
    private final AtomicReference<Map<EndpointsSelector, Collection<String>>> addresses = new AtomicReference<>(new HashMap<>());

    public NeptuneEndpointsInfoLambda() {

        String clusterId = EnvironmentVariableUtils.getMandatoryEnv("clusterId");
        int pollingIntervalSeconds = Integer.parseInt(
                EnvironmentVariableUtils.getOptionalEnv("pollingIntervalSeconds", "15"));

        refreshAgent = new ClusterEndpointsRefreshAgent(clusterId,
                EndpointsType.All,
                EndpointsType.Primary,
                EndpointsType.ReadReplicas,
                EndpointsType.ClusterEndpoint,
                EndpointsType.ReaderEndpoint);

        addresses.set(refreshAgent.getAddresses());

        refreshAgent.startPollingNeptuneAPI(
                addresses::set,
                pollingIntervalSeconds,
                TimeUnit.SECONDS);
    }


    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {

        LambdaLogger logger = context.getLogger();

        EndpointsType endpointsType = EndpointsType.ReadReplicas;

        Scanner scanner = new Scanner(input);
        if (scanner.hasNext()) {
            String param = scanner.next().replace("\"", "");
            if (!param.isEmpty()) {
                endpointsType = EndpointsType.valueOf(param);
            }
        }

        logger.log("EndpointsType: " + endpointsType);

        Map<EndpointsSelector, Collection<String>> addressesMap = addresses.get();

        for (Map.Entry<EndpointsSelector, Collection<String>> entry : addressesMap.entrySet()) {
            logger.log(entry.getKey() + ": " + entry.getValue());
        }

        String results = String.join(",", addressesMap.get(endpointsType));
        logger.log("Results: " + results);

        try (Writer writer = new BufferedWriter(new OutputStreamWriter(output, UTF_8))) {
            writer.write(results);
            writer.flush();
        }

    }
}
