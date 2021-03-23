/*
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.rdf.io;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.neptune.auth.NeptuneApacheHttpSigV4Signer;
import com.amazonaws.neptune.auth.NeptuneSigV4Signer;
import com.amazonaws.neptune.auth.NeptuneSigV4SignerException;
import com.amazonaws.services.neptune.cluster.ConnectionConfig;
import org.apache.http.HttpException;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.HttpClientBuilder;
import org.eclipse.rdf4j.http.client.util.HttpClientBuilders;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;

public class NeptuneExportSparqlRepository extends SPARQLRepository {
    private final String regionName;
    private final AWSCredentialsProvider awsCredentialsProvider;
    private final ConnectionConfig config;
    private NeptuneSigV4Signer<HttpUriRequest> v4Signer;

    public NeptuneExportSparqlRepository(String endpointUrl) throws NeptuneSigV4SignerException {
        this(endpointUrl, null, null, null);
    }

    public NeptuneExportSparqlRepository(String endpointUrl, AWSCredentialsProvider awsCredentialsProvider, String regionName, ConnectionConfig config) throws NeptuneSigV4SignerException {
        super(getSparqlEndpoint(endpointUrl));
        this.config = config;
        this.awsCredentialsProvider = awsCredentialsProvider;
        this.regionName = regionName;
        this.initAuthenticatingHttpClient();
    }

    protected void initAuthenticatingHttpClient() throws NeptuneSigV4SignerException {

        HttpClientBuilder httpClientBuilder = config.useSsl() ?
                HttpClientBuilders.getSSLTrustAllHttpClientBuilder() :
                HttpClientBuilder.create();

        if (config.useIamAuth()) {
            v4Signer = new NeptuneApacheHttpSigV4Signer(regionName, awsCredentialsProvider);
            HttpClient v4SigningClient = httpClientBuilder.addInterceptorLast((HttpRequestInterceptor) (req, ctx) -> {
                if (req instanceof HttpUriRequest) {
                    HttpUriRequest httpUriReq = (HttpUriRequest) req;

                    try {
                        v4Signer.signRequest(httpUriReq);
                    } catch (NeptuneSigV4SignerException var5) {
                        throw new HttpException("Problem signing the request: ", var5);
                    }
                } else {
                    throw new HttpException("Not an HttpUriRequest");
                }
            }).build();
            setHttpClient(v4SigningClient);
        } else {
            setHttpClient(httpClientBuilder.build());
        }
    }

    private static String getSparqlEndpoint(String endpointUrl) {
        return endpointUrl + "/sparql";
    }
}
