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
