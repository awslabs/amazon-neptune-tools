/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

package org.apache.tinkerpop.gremlin.driver;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.*;

public class IamAuthConfig {

    public static final String DEFAULT_PROFILE = "default";

    public static IamAuthConfigBuilder builder(){
        return new IamAuthConfigBuilder();
    }

    private final List<String> endpoints;
    private final int port;
    private final boolean connectViaLoadBalancer;
    private final boolean removeHostHeaderAfterSigning;
    private final String serviceRegion;
    private final String iamProfile;
    private final AWSCredentialsProvider credentials;
    private final Random random = new Random(DateTime.now().getMillis());

    public IamAuthConfig(Collection<String> endpoints,
                         int port,
                         boolean connectViaLoadBalancer,
                         boolean removeHostHeaderAfterSigning,
                         String serviceRegion,
                         String iamProfile,
                         AWSCredentialsProvider credentials) {
        this.endpoints = new ArrayList<>(endpoints);
        this.port = port;
        this.connectViaLoadBalancer = connectViaLoadBalancer;
        this.removeHostHeaderAfterSigning = removeHostHeaderAfterSigning;
        this.serviceRegion = serviceRegion;
        this.iamProfile = iamProfile;
        this.credentials = credentials;
    }

    public String serviceRegion() {
        return serviceRegion;
    }

    public AWSCredentialsProviderChain credentialsProviderChain(){
        if (credentials != null){
            return new AWSCredentialsProviderChain(Collections.singletonList(credentials));
        } else if (!iamProfile.equals(DEFAULT_PROFILE)){
            return new AWSCredentialsProviderChain(Collections.singletonList(new ProfileCredentialsProvider(iamProfile)));
        } else {
            return new DefaultAWSCredentialsProviderChain();
        }
    }

    public String chooseHostHeader() {
        return String.format("%s:%s", endpoints.get(random.nextInt(endpoints.size())), port);
    }

    public boolean connectViaLoadBalancer() {
        return connectViaLoadBalancer;
    }

    public boolean removeHostHeaderAfterSigning() {
        return removeHostHeaderAfterSigning;
    }

    public String asJsonString() {
        ObjectNode json = JsonNodeFactory.instance.objectNode();

        ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
        for (String endpoint : endpoints) {
            arrayNode.add(endpoint);
        }

        json.set("endpoints", arrayNode);
        json.put("port", port);
        json.put("connectViaLoadBalancer", connectViaLoadBalancer);
        json.put("removeHostHeaderAfterSigning", removeHostHeaderAfterSigning);
        json.put("serviceRegion", serviceRegion);
        json.put("iamProfile", iamProfile);

        return json.toString();
    }

    @Override
    public String toString() {
        return asJsonString();
    }

    public static final class IamAuthConfigBuilder {

        private final List<String> endpoints = new ArrayList<>();
        private int port = 8182;
        private boolean connectViaLoadBalancer = false;
        private boolean removeHostHeaderAfterSigning = false;
        private String serviceRegion = "";
        private String iamProfile = DEFAULT_PROFILE;
        private AWSCredentialsProvider credentials = null;

        public IamAuthConfigBuilder addNeptuneEndpoints(String... endpoints){
            this.endpoints.addAll(Arrays.asList(endpoints));
            return this;
        }

        public IamAuthConfigBuilder addNeptuneEndpoints(List<String> endpoints){
            this.endpoints.addAll(endpoints);
            return this;
        }

        public IamAuthConfigBuilder setNeptunePort(int port){
            this.port = port;
            return this;
        }

        public IamAuthConfigBuilder setServiceRegion(String serviceRegion){
            this.serviceRegion = serviceRegion;
            return this;
        }

        public IamAuthConfigBuilder setIamProfile(String iamProfile){
            this.iamProfile = iamProfile;
            return this;
        }

        public IamAuthConfigBuilder setCredentials(AWSCredentialsProvider credentials){
            this.credentials = credentials;
            return this;
        }

        public IamAuthConfigBuilder removeHostHeaderAfterSigning(){
            this.removeHostHeaderAfterSigning = true;
            return this;
        }

        public IamAuthConfigBuilder connectViaLoadBalancer(){
            this.connectViaLoadBalancer = true;
            return this;
        }

        public IamAuthConfig build(){
            return new IamAuthConfig(
                    endpoints,
                    port,
                    connectViaLoadBalancer,
                    removeHostHeaderAfterSigning,
                    serviceRegion,
                    iamProfile,
                    credentials);
        }
    }
}
