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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.model.TooManyRequestsException;
import com.evanlennick.retry4j.CallExecutor;
import com.evanlennick.retry4j.CallExecutorBuilder;
import com.evanlennick.retry4j.Status;
import com.evanlennick.retry4j.config.RetryConfig;
import com.evanlennick.retry4j.config.RetryConfigBuilder;
import org.apache.commons.lang.StringUtils;
import software.amazon.utils.RegionUtils;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class GetEndpointsFromLambdaProxy implements ClusterEndpointsFetchStrategy {

    private final EndpointsType endpointsType;
    private final String lambdaName;
    private final AWSLambda lambdaClient;
    private final RetryConfig retryConfig;

    public GetEndpointsFromLambdaProxy(EndpointsType endpointsType, String lambdaName) {
        this(endpointsType, RegionUtils.getCurrentRegionName(), lambdaName);
    }

    public GetEndpointsFromLambdaProxy(EndpointsType endpointsType, String region, String lambdaName) {
        this.endpointsType = endpointsType;
        this.lambdaName = lambdaName;
        this.lambdaClient = createLambdaClient(region);
        this.retryConfig = new RetryConfigBuilder()
                .retryOnSpecificExceptions(TooManyRequestsException.class)
                .withMaxNumberOfTries(10)
                .withDelayBetweenTries(10, ChronoUnit.MILLIS)
                .withExponentialBackoff()
                .build();
    }

    private AWSLambda createLambdaClient(String region) {
        AWSLambdaClientBuilder builder = AWSLambdaClientBuilder.standard()
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance());
        if (StringUtils.isNotEmpty(region)){
            return builder.withRegion(region).build();
        } else {
            return builder.build();
        }
    }

    @Override
    public Map<EndpointsSelector, Collection<String>> getAddresses() {

        Callable<Map<EndpointsSelector, Collection<String>>> query = () -> {
            InvokeRequest invokeRequest = new InvokeRequest()
                    .withFunctionName(lambdaName)
                    .withPayload(String.format("\"%s\"", endpointsType.name()));

            InvokeResult result = lambdaClient.invoke(invokeRequest);
            String payload = new String(result.getPayload().array());

            Map<EndpointsSelector, Collection<String>> results = new HashMap<>();

            results.put(endpointsType, Arrays.asList(payload.split(",")));

            return results;
        };

        @SuppressWarnings("unchecked")
        CallExecutor<Map<EndpointsSelector, Collection<String>>> executor =
                new CallExecutorBuilder<Map<EndpointsSelector, Collection<String>>>().config(retryConfig).build();

        Status<Map<EndpointsSelector, Collection<String>>> status = executor.execute(query);

        return status.getResult();
    }
}
