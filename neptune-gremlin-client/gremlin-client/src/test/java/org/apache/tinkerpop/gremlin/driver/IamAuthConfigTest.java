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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class IamAuthConfigTest {

    @Test
    public void shouldCreateJson() {
        IamAuthConfig config = new IamAuthConfig.IamAuthConfigBuilder()
                .addNeptuneEndpoints("endpoint1", "endpoint2")
                .setNeptunePort(8182)
                .connectViaLoadBalancer()
                .removeHostHeaderAfterSigning()
                .setIamProfile("neptune")
                .setServiceRegion("us-east-1")
                .build();

        String json = config.asJsonString();

        assertEquals("{\"endpoints\":[\"endpoint1\",\"endpoint2\"],\"port\":8182,\"connectViaLoadBalancer\":true,\"removeHostHeaderAfterSigning\":true,\"serviceRegion\":\"us-east-1\",\"iamProfile\":\"neptune\"}", json);
    }

}