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

package com.amazonaws.services.neptune;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.neptune.export.NeptuneExportLambda;
import com.amazonaws.services.neptune.util.NotImplementedException;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@Command(name = "nesvc", description = "neptune-export service", hidden = true)
public class RunNeptuneExportSvc extends NeptuneExportBaseCommand implements Runnable {

    /**
     * Same as the default value given in the CFN template at https://docs.aws.amazon.com/neptune/latest/userguide/export-service.html
     */
    public static final int DEFAULT_MAX_FILE_DESCRIPTOR_COUNT = 10000;

    @Option(name = {"--json"}, description = "JSON")
    @Once
    private String json;

    @Option(name = {"--root-path"}, description = "Root directory path", hidden = true)
    @Once
    private String rootPath = new File("exports").getAbsolutePath();

    @Option(name = {"--clean"}, description = "Clean output path before beginning an export.", hidden = true)
    @Once
    private boolean cleanRootPath = false;

    @Option(name = {"--max-file-descriptor-count"}, description = "Maximum number of simultaneously open files.", hidden = true)
    @Once
    private int maxFileDescriptorCount = DEFAULT_MAX_FILE_DESCRIPTOR_COUNT;

    @Override
    public void run() {

        InputStream input = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));

        try {
            new NeptuneExportLambda(rootPath, cleanRootPath, maxFileDescriptorCount).handleRequest(input, System.out, new Context() {
                @Override
                public String getAwsRequestId() {
                    throw new NotImplementedException();
                }

                @Override
                public String getLogGroupName() {
                    throw new NotImplementedException();
                }

                @Override
                public String getLogStreamName() {
                    throw new NotImplementedException();
                }

                @Override
                public String getFunctionName() {
                    throw new NotImplementedException();
                }

                @Override
                public String getFunctionVersion() {
                    throw new NotImplementedException();
                }

                @Override
                public String getInvokedFunctionArn() {
                    throw new NotImplementedException();
                }

                @Override
                public CognitoIdentity getIdentity() {
                    throw new NotImplementedException();
                }

                @Override
                public ClientContext getClientContext() {
                    throw new NotImplementedException();
                }

                @Override
                public int getRemainingTimeInMillis() {
                    throw new NotImplementedException();
                }

                @Override
                public int getMemoryLimitInMB() {
                    throw new NotImplementedException();
                }

                @Override
                public LambdaLogger getLogger() {
                    return new LambdaLogger() {
                        @Override
                        public void log(String s) {
                            System.out.println(s);
                        }

                        @Override
                        public void log(byte[] bytes) {
                            throw new NotImplementedException();
                        }
                    };
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("An error occurred while exporting from Neptune: " + e.getMessage());
            System.exit(-1);
        }

        System.exit(0);
    }
}
