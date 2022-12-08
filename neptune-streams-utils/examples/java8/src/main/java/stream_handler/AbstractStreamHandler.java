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

package stream_handler;

import com.amazonaws.neptune.StreamRecordsHandler;
import com.amazonaws.neptune.config.CredentialsConfig;

import java.util.Map;

public abstract class AbstractStreamHandler implements StreamRecordsHandler {

    protected final String neptuneEndpoint;
    protected final Integer neptunePort;
    protected final CredentialsConfig credentialsConfig;
    protected final Map<String, Object> additionalParams;

    public AbstractStreamHandler(String neptuneEndpoint, Integer neptunePort, CredentialsConfig credentialsConfig, Map<String, Object> additionalParams) {
        this.neptuneEndpoint = neptuneEndpoint;
        this.neptunePort = neptunePort;
        this.credentialsConfig = credentialsConfig;
        this.additionalParams = additionalParams;
    }
}
