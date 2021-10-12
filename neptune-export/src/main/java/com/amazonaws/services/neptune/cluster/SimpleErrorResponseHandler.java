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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.http.HttpResponse;
import com.amazonaws.http.HttpResponseHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.util.Scanner;

public class SimpleErrorResponseHandler implements HttpResponseHandler<AmazonServiceException> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public AmazonServiceException handle(HttpResponse response) throws Exception {

        String content = null;

        InputStream stream = response.getContent();

        if (stream != null) {
            Scanner s = new Scanner(stream).useDelimiter("\\A");
            content = s.hasNext() ? s.next() : "";
        }

        AmazonServiceException.ErrorType errorType = AmazonServiceException.ErrorType.Unknown;

        if (response.getStatusCode() >= 500){
            errorType = AmazonServiceException.ErrorType.Service;
        } else if (response.getStatusCode() >= 400){
            errorType = AmazonServiceException.ErrorType.Client;
        }

        String errorCode = "UnknownError";
        String message = response.getStatusText();
        String requestId = "";

        if (content != null){
            JsonNode json = MAPPER.readTree(content);
            if (json.has("requestId")){
                requestId = json.path("requestId").textValue();
            }
            if (json.has("code")){
                errorCode = json.path("code").textValue();
            }
            if (json.has("detailedMessage")){
                message = json.path("detailedMessage").textValue();
            }
        }

        AmazonServiceException exception = new AmazonServiceException(message);
        exception.setStatusCode(response.getStatusCode());
        exception.setRequestId(requestId);
        exception.setErrorType(errorType);
        exception.setErrorCode(errorCode);

        return exception;
    }

    @Override
    public boolean needsConnectionLeftOpen() {
        return false;
    }
}
