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

import com.amazon.neptune.gremlin.driver.exception.SigV4PropertiesNotFoundException;
import com.amazon.neptune.gremlin.driver.sigv4.ChainedSigV4PropertiesProvider;
import com.amazon.neptune.gremlin.driver.sigv4.SigV4Properties;
import com.amazonaws.neptune.auth.NeptuneNettyHttpSigV4Signer;
import com.amazonaws.neptune.auth.NeptuneSigV4SignerException;
import io.netty.handler.codec.http.FullHttpRequest;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.utils.RegionUtils;

public class LBAwareHandshakeInterceptor implements HandshakeInterceptor {

    private static final Logger logger = LoggerFactory.getLogger(LBAwareHandshakeInterceptor.class);

    private final IamAuthConfig iamAuthConfig;
    private final SigV4Properties sigV4Properties;

    public LBAwareHandshakeInterceptor(IamAuthConfig iamAuthConfig, ChainedSigV4PropertiesProvider sigV4PropertiesProvider) {
        this.iamAuthConfig = iamAuthConfig;
        this.sigV4Properties = loadProperties(sigV4PropertiesProvider);
    }

    @Override
    public FullHttpRequest apply(FullHttpRequest request) {
        logger.trace("IamAuthConfig: {}", iamAuthConfig);

        if (iamAuthConfig.connectViaLoadBalancer()){
            request.headers().remove("Host");
            request.headers().add("Host", iamAuthConfig.chooseHostHeader());
        }

        try {

            NeptuneNettyHttpSigV4Signer sigV4Signer = new NeptuneNettyHttpSigV4Signer(
                    sigV4Properties.getServiceRegion(),
                    iamAuthConfig.credentialsProviderChain());

            sigV4Signer.signRequest(request);

            if (iamAuthConfig.removeHostHeaderAfterSigning()) {
                request.headers().remove("Host");
            }

            return request;

        } catch (NeptuneSigV4SignerException e) {
            throw new RuntimeException("Exception occurred while signing the request", e);
        }
    }

    private SigV4Properties loadProperties(ChainedSigV4PropertiesProvider sigV4PropertiesProvider) {

        if (StringUtils.isNotEmpty(iamAuthConfig.serviceRegion())) {
            return new SigV4Properties(iamAuthConfig.serviceRegion());
        }

        try {
            return sigV4PropertiesProvider.getSigV4Properties();
        } catch (SigV4PropertiesNotFoundException e) {
            String currentRegionName = RegionUtils.getCurrentRegionName();
            if (currentRegionName != null) {
                logger.info("Creating SigV4 properties using current region");
                return new SigV4Properties(currentRegionName);
            } else {
                throw new IllegalStateException("Unable to determine Neptune service region. Use the SERVICE_REGION environment variable or system property, or the NeptuneGremlinClusterBuilder.serviceRegion() method to specify the Neptune service region.");
            }
        }
    }
}
