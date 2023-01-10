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

package com.amazonaws.services.neptune.cluster;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

public class GetLastEventId {

    public static final String MaxCommitNumValueForEngine(String engineVersion){
        List<Integer> parts = Arrays.stream(engineVersion.split("\\."))
                .mapToInt(Integer::valueOf)
                .boxed()
                .collect(Collectors.toList());



        if (parts.get(1) == 0){
            if (parts.get(2) < 4){
                return String.valueOf(Integer.MAX_VALUE);
            } else if (parts.get(2) == 4 && parts.get(3) < 2){
                return String.valueOf(Integer.MAX_VALUE);
            } else {
                return String.valueOf(Long.MAX_VALUE);
            }
        } else {
            return String.valueOf(Long.MAX_VALUE);
        }
    }

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(GetLastEventId.class);

    private final NeptuneClusterMetadata clusterMetadata;
    private final ConnectionConfig connectionConfig;
    private final String streamEndpointType;

    public GetLastEventId(NeptuneClusterMetadata clusterMetadata, ConnectionConfig connectionConfig, String streamEndpointType) {
        this.clusterMetadata = clusterMetadata;
        this.connectionConfig = connectionConfig;
        this.streamEndpointType = streamEndpointType;
    }

    public EventId execute() {

        if (!clusterMetadata.isStreamEnabled()){
            return null;
        }

        String endpoint = connectionConfig.endpoints().iterator().next();

        String streamsEndpoint = String.format("https://%s:%s/%s/stream", endpoint, connectionConfig.port(), streamEndpointType);
        logger.info("Streams endpoint: {}", streamsEndpoint);

        try {

            String region = new DefaultAwsRegionProviderChain().getRegion();
            NeptuneHttpsClient neptuneHttpsClient = new NeptuneHttpsClient(streamsEndpoint, region, endpoint.equals("localhost"));

            Map<String, String> params = new HashMap<>();
            params.put("commitNum", MaxCommitNumValueForEngine(clusterMetadata.engineVersion()));
            params.put("limit", "1");

            HttpResponse httpResponse = neptuneHttpsClient.get(params);

            logger.info(httpResponse.getContent());

            return null;

        } catch (AmazonServiceException e) {

            if (e.getErrorCode().equals("StreamRecordsNotFoundException")) {
                EventId lastEventId = StreamRecordsNotFoundExceptionParser.parseLastEventId(e.getErrorMessage());
                logger.info("LastEventId: {}", lastEventId);
                return lastEventId;
            } else {
                logger.error("Error while accessing Neptune Streams endpoint", e);
                return null;
            }

        } catch (Exception e) {
            logger.error("Error while accessing Neptune Streams endpoint", e);
            return null;
        }
    }
}
