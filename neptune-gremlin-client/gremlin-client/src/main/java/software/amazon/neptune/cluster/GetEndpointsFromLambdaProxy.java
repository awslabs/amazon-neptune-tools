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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
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
import com.evanlennick.retry4j.exception.UnexpectedException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.driver.IamAuthConfig;
import software.amazon.utils.RegionUtils;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class GetEndpointsFromLambdaProxy implements ClusterEndpointsFetchStrategy {

    private final EndpointsSelector endpointsSelector;
    private final String lambdaName;
    private final AWSLambda lambdaClient;
    private final RetryConfig retryConfig;

    public GetEndpointsFromLambdaProxy(EndpointsSelector endpointsSelector, String lambdaName) {
        this(endpointsSelector, lambdaName, RegionUtils.getCurrentRegionName());
    }

    public GetEndpointsFromLambdaProxy(EndpointsSelector endpointsSelector, String lambdaName, String region) {
        this(endpointsSelector, lambdaName, region, IamAuthConfig.DEFAULT_PROFILE);
    }

    public GetEndpointsFromLambdaProxy(EndpointsSelector endpointsSelector,
                                       String lambdaName,
                                       String region,
                                       String iamProfile) {
        this(endpointsSelector, lambdaName, region, iamProfile, null);
    }

    public GetEndpointsFromLambdaProxy(EndpointsSelector endpointsSelector,
                                       String lambdaName,
                                       String region,
                                       AWSCredentialsProvider credentials) {
        this(endpointsSelector, lambdaName, region, IamAuthConfig.DEFAULT_PROFILE, credentials);
    }

    public GetEndpointsFromLambdaProxy(EndpointsType endpointsType, String lambdaName) {
        this(endpointsType, lambdaName, RegionUtils.getCurrentRegionName());
    }

    public GetEndpointsFromLambdaProxy(EndpointsType endpointsType, String lambdaName, String region) {
        this(endpointsType, lambdaName, region, IamAuthConfig.DEFAULT_PROFILE);
    }

    public GetEndpointsFromLambdaProxy(EndpointsType endpointsType,
                                       String lambdaName,
                                       String region,
                                       String iamProfile) {
        this(endpointsType, lambdaName, region, iamProfile, null);
    }

    public GetEndpointsFromLambdaProxy(EndpointsType endpointsType,
                                       String lambdaName,
                                       String region,
                                       AWSCredentialsProvider credentials) {
        this(endpointsType, lambdaName, region, IamAuthConfig.DEFAULT_PROFILE, credentials);
    }

    private GetEndpointsFromLambdaProxy(EndpointsSelector endpointsSelector,
                                        String lambdaName,
                                        String region,
                                        String iamProfile,
                                        AWSCredentialsProvider credentials) {
        this.endpointsSelector = endpointsSelector;
        this.lambdaName = lambdaName;
        this.lambdaClient = createLambdaClient(region, iamProfile, credentials);
        this.retryConfig = new RetryConfigBuilder()
                .retryOnSpecificExceptions(TooManyRequestsException.class)
                .withMaxNumberOfTries(10)
                .withDelayBetweenTries(10, ChronoUnit.MILLIS)
                .withExponentialBackoff()
                .build();
    }

    private AWSLambda createLambdaClient(String region, String iamProfile, AWSCredentialsProvider credentials) {
        AWSLambdaClientBuilder builder = AWSLambdaClientBuilder.standard();

        if (credentials != null) {
            builder = builder.withCredentials(credentials);
        } else {

            if (!iamProfile.equals(IamAuthConfig.DEFAULT_PROFILE)) {
                builder = builder.withCredentials(new ProfileCredentialsProvider(iamProfile));
            } else {
                builder = builder.withCredentials(DefaultAWSCredentialsProviderChain.getInstance());
            }
        }

        if (StringUtils.isNotEmpty(region)) {
            builder = builder.withRegion(region);
        }

        return builder.build();
    }

    @Override
    public Map<EndpointsSelector, Collection<String>> getAddresses() {

        if (EndpointsType.class.isAssignableFrom(endpointsSelector.getClass())) {
            return getAddressesForEndpointsType();
        } else {
            return getAddressesForCustomEndpointsSelector();
        }
    }

    private Map<EndpointsSelector, Collection<String>> getAddressesForCustomEndpointsSelector() {
        Callable<NeptuneClusterMetadata> query = () -> {

            InvokeRequest invokeRequest = new InvokeRequest()
                    .withFunctionName(lambdaName)
                    .withPayload("\"\"");
            InvokeResult result = lambdaClient.invoke(invokeRequest);

            return NeptuneClusterMetadata.fromByeArray(result.getPayload().array());
        };

        @SuppressWarnings("unchecked")
        CallExecutor<NeptuneClusterMetadata> executor =
                new CallExecutorBuilder<Map<EndpointsSelector, Collection<String>>>().config(retryConfig).build();

        Status<NeptuneClusterMetadata> status;

        try {
            status = executor.execute(query);
        } catch (UnexpectedException e){
            if (e.getCause() instanceof MismatchedInputException){
                throw new IllegalStateException(String.format("The AWS Lambda proxy (%s) isn't returning a NeptuneClusterMetadata JSON document. Check that the function supports returning a NeptuneClusterMetadata JSON document.", lambdaName), e.getCause());
            } else{
                throw new IllegalStateException(String.format("There was an unexpected error while attempting to get a NeptuneClusterMetadata JSON document from the AWS Lambda proxy (%s). Check that the function supports returning a NeptuneClusterMetadata JSON document.", lambdaName), e.getCause());
            }
        }

        NeptuneClusterMetadata neptuneClusterMetadata = status.getResult();
        Map<EndpointsSelector, Collection<String>> results = new HashMap<>();

        results.put(endpointsSelector, endpointsSelector.getEndpoints(
                neptuneClusterMetadata.getClusterEndpoint(),
                neptuneClusterMetadata.getReaderEndpoint(),
                neptuneClusterMetadata.getInstances()));

        return results;
    }

    private Map<EndpointsSelector, Collection<String>> getAddressesForEndpointsType() {
        Callable<Map<EndpointsSelector, Collection<String>>> query = () -> {

            EndpointsType endpointsType = (EndpointsType) endpointsSelector;

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

        Status<Map<EndpointsSelector, Collection<String>>> status;

        try{
            status = executor.execute(query);
        } catch (UnexpectedException e){
            throw new IllegalStateException(String.format("There was an unexpected error while attempting to get a list of endpoints from the AWS Lambda proxy (%s). Check that the function supports returning a list of endpoints.", lambdaName), e.getCause());
        }

        return status.getResult();
    }
}
