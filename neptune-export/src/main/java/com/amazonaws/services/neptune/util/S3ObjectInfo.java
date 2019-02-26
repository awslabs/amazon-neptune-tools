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

package com.amazonaws.services.neptune.util;

import java.io.File;
import java.net.URI;

public class S3ObjectInfo {
    private final String bucket;
    private final String key;
    private final String fileName;

    public S3ObjectInfo(String s3Uri) {
        URI uri = URI.create(s3Uri);

        bucket = uri.getAuthority();
        key = uri.getPath().substring(1);
        fileName = new File(uri.getPath()).getName();
    }

    public String bucket() {
        return bucket;
    }

    public String key() {
        return key;
    }

    public File createDownloadFile(String parent) {
        return new File(parent, fileName);
    }

    public S3ObjectInfo withNewKeySuffix(String suffix) {
        return new S3ObjectInfo( String.format("s3://%s/%s", bucket,  new File(key, suffix).getPath()));
    }

    @Override
    public String toString() {
        return String.format("s3://%s/%s", bucket, key);
    }
}
