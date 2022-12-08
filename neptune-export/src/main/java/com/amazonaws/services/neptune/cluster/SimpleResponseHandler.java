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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.http.HttpResponseHandler;

import java.io.InputStream;
import java.util.Scanner;

class SimpleResponseHandler implements HttpResponseHandler<HttpResponse> {

    @Override
    public HttpResponse handle(com.amazonaws.http.HttpResponse response) {

        int status = response.getStatusCode();
        String content = null;

        InputStream stream = response.getContent();

        if (stream != null){
            Scanner s = new Scanner(stream).useDelimiter("\\A");
            content = s.hasNext() ? s.next() : "";
        }

        if (status < 200 || status >= 300) {

            AmazonServiceException ase = new AmazonServiceException(content);
            ase.setStatusCode(status);
            throw ase;
        }

        String contentType = response.getHeaderValues("content-type").get(0);

        return new HttpResponse(status, content, contentType);
    }

    @Override
    public boolean needsConnectionLeftOpen() {
        return false;
    }

}
