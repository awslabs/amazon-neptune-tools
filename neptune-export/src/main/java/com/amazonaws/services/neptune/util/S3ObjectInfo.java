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

import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.net.URI;

public class S3ObjectInfo {
    private final String bucket;
    private final String key;
    private final String fileName;

    public S3ObjectInfo(String s3Uri) {
        URI uri = URI.create(s3Uri);

        bucket = uri.getAuthority();
        String path = uri.getPath();
        key = StringUtils.isNotEmpty(path) ? path.substring(1) : "";
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
        File file = StringUtils.isNotEmpty(key) ? new File(key, suffix) : new File(suffix);
        return new S3ObjectInfo( String.format("s3://%s/%s", bucket,  file.getPath()));
    }

    public S3ObjectInfo replaceOrAppendKey(String placeholder, String ifPresent, String ifAbsent) {

        File file = key.contains(placeholder) ?
                new File(key.replace(placeholder, ifPresent)) :
                new File(key, ifAbsent);

        return new S3ObjectInfo( String.format("s3://%s/%s", bucket,  file.getPath()));
    }

    public S3ObjectInfo replaceOrAppendKey(String placeholder, String ifPresent) {
        return replaceOrAppendKey(placeholder, ifPresent, ifPresent);
    }

    @Override
    public String toString() {
        return String.format("s3://%s/%s", bucket, key);
    }

}
