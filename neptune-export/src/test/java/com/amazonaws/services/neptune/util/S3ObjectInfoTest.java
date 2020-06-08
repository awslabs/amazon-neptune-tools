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

import com.amazonaws.services.neptune.cluster.InstanceType;
import com.amazonaws.services.neptune.propertygraph.NamedQueriesCollection;
import com.amazonaws.services.neptune.propertygraph.NamedQuery;
import com.amazonaws.services.neptune.propertygraph.io.JsonResource;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import static org.junit.Assert.assertEquals;

public class S3ObjectInfoTest {

    @Test
    public void canParseBucketFromURI(){
        String s3Uri = "s3://my-bucket/a/b/c";

        S3ObjectInfo s3ObjectInfo = new S3ObjectInfo(s3Uri);

        assertEquals("my-bucket", s3ObjectInfo.bucket());
    }

    @Test
    public void canParseKeyWithoutTrailingSlashFromURI(){
        String s3Uri = "s3://my-bucket/a/b/c";

        S3ObjectInfo s3ObjectInfo = new S3ObjectInfo(s3Uri);

        assertEquals("a/b/c", s3ObjectInfo.key());
    }

    @Test
    public void canParseKeyWithTrainlingSlashFromURI(){
        String s3Uri = "s3://my-bucket/a/b/c/";

        S3ObjectInfo s3ObjectInfo = new S3ObjectInfo(s3Uri);

        assertEquals("a/b/c/", s3ObjectInfo.key());
    }

    @Test
    public void canCreateDownloadFileForKeyWithoutTrailingSlash(){
        String s3Uri = "s3://my-bucket/a/b/c.txt";

        S3ObjectInfo s3ObjectInfo = new S3ObjectInfo(s3Uri);

        assertEquals("/temp/c.txt", s3ObjectInfo.createDownloadFile("/temp").getAbsolutePath());
    }

    @Test
    public void canCreateDownloadFileForKeyWithTrailingSlash(){
        String s3Uri = "s3://my-bucket/a/b/c/";

        S3ObjectInfo s3ObjectInfo = new S3ObjectInfo(s3Uri);

        assertEquals("/temp/c", s3ObjectInfo.createDownloadFile("/temp").getAbsolutePath());
    }

    @Test
    public void canCreateNewInfoForKeyWithoutTrailingSlash() {
        String s3Uri = "s3://my-bucket/a/b/c";

        S3ObjectInfo s3ObjectInfo = new S3ObjectInfo(s3Uri);

        assertEquals("a/b/c/dir", s3ObjectInfo.withNewKeySuffix("dir").key());
    }

    @Test
    public void canCreateNewKeyForKeyWithTrailingSlash() {
        String s3Uri = "s3://my-bucket/a/b/c/";

        S3ObjectInfo s3ObjectInfo = new S3ObjectInfo(s3Uri);

        assertEquals("a/b/c/dir", s3ObjectInfo.withNewKeySuffix("dir").key());
    }
}