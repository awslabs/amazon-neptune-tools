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

package com.amazonaws.services.neptune.cli;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.neptune.AmazonNeptune;
import com.amazonaws.services.neptune.AmazonNeptuneClientBuilder;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;
import org.apache.commons.lang.StringUtils;

import java.util.function.Supplier;

public class AwsCliModule implements Supplier<AmazonNeptune> {
    @Option(name = {"--aws-cli-endpoint-url"}, description = "AWS CLI endpoint URL.", hidden = true)
    @Once
    private String awsCliEndpointUrl;

    @Option(name = {"--aws-cli-region"}, description = "AWS CLI region.", hidden = true)
    @Once
    private String awsCliRegion;

    @Override
    public AmazonNeptune get() {
        return StringUtils.isNotEmpty(awsCliEndpointUrl) && StringUtils.isNotEmpty(awsCliRegion) ?
                AmazonNeptuneClientBuilder.standard().withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(awsCliEndpointUrl, awsCliRegion)).build() :
                AmazonNeptuneClientBuilder.defaultClient();
    }

}
