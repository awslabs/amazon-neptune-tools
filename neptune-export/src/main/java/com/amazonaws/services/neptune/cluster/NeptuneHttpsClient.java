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

package com.amazonaws.services.neptune.cluster;

import com.amazonaws.*;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.AmazonHttpClient;
import com.amazonaws.http.ExecutionContext;
import com.amazonaws.http.HttpMethodName;

import java.net.URI;
import java.util.Map;

public class NeptuneHttpsClient {

    private final AWSCredentialsProvider awsCredentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();

    private final AWS4Signer signer;
    private final String uri;
    private final boolean disableCertCheck;

    public NeptuneHttpsClient(String uri, String region, boolean disableCertCheck) {
        this.uri = uri;
        this.disableCertCheck = disableCertCheck;
        signer = new AWS4Signer();
        signer.setRegionName(region);
        signer.setServiceName("neptune-db");
    }

    public HttpResponse get(Map<String, String> queryStringParams) {

        Request<Void> request = new DefaultRequest<>(signer.getServiceName());
        request.setEndpoint(URI.create(uri));
        request.setHttpMethod(HttpMethodName.GET);

        for (Map.Entry<String, String> entry : queryStringParams.entrySet()) {
            request.addParameter(entry.getKey(), entry.getValue());
        }

        signer.sign(request, awsCredentialsProvider.getCredentials());

        if (disableCertCheck){
            System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
        }

        Response<HttpResponse> response = new AmazonHttpClient(new ClientConfiguration())
                .requestExecutionBuilder()
                .executionContext(new ExecutionContext(false))
                .request(request)
                .errorResponseHandler(new SimpleErrorResponseHandler())
                .execute(new SimpleResponseHandler());

        return response.getAwsResponse();
    }
}
