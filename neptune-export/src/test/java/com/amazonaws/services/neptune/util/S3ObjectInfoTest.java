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

import org.junit.Test;

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

    @Test
    public void canReplacePlaceholderInKey() {
        String s3Uri = "s3://my-bucket/a/b/_COMPLETION_ID_/manifest.json";

        S3ObjectInfo s3ObjectInfo = new S3ObjectInfo(s3Uri);

        assertEquals("a/b/123/manifest.json", s3ObjectInfo.replaceOrAppendKey("_COMPLETION_ID_", "123").key());
    }

    @Test
    public void canReplaceTmpPlaceholderInKey() {
        String s3Uri = "s3://my-bucket/a/b/tmp/manifest.json";

        S3ObjectInfo s3ObjectInfo = new S3ObjectInfo(s3Uri);

        assertEquals("a/b/failed/manifest.json", s3ObjectInfo.replaceOrAppendKey("/tmp/", "/failed/").key());
    }

    @Test
    public void canAppendSuffixIfNoPlaceholder() {
        String s3Uri = "s3://my-bucket/a/b/";

        S3ObjectInfo s3ObjectInfo = new S3ObjectInfo(s3Uri);

        assertEquals("a/b/123", s3ObjectInfo.replaceOrAppendKey("_COMPLETION_ID_", "123").key());
    }

    @Test
    public void canAppendAltSuffixIfNoPlaceholder() {
        String s3Uri = "s3://my-bucket/a/b/";

        S3ObjectInfo s3ObjectInfo = new S3ObjectInfo(s3Uri);

        assertEquals("a/b/123.json", s3ObjectInfo.replaceOrAppendKey("_COMPLETION_ID_", "123", "123.json").key());
    }

    @Test
    public void canHandlePathsWithBucketNameOnlyNoSlash(){
        String s3Uri = "s3://my-bucket";

        S3ObjectInfo s3ObjectInfo = new S3ObjectInfo(s3Uri);

        assertEquals("", s3ObjectInfo.key());
        assertEquals("s3://my-bucket/new-suffix", s3ObjectInfo.withNewKeySuffix("new-suffix").toString());
        assertEquals("new-suffix", s3ObjectInfo.withNewKeySuffix("new-suffix").key());
        assertEquals("/123", s3ObjectInfo.replaceOrAppendKey("_COMPLETION_ID_", "123").key());
        assertEquals("/123.json", s3ObjectInfo.replaceOrAppendKey("_COMPLETION_ID_", "123", "123.json").key());
    }

    @Test
    public void canHandlePathsWithBucketNameWithSlash(){
        String s3Uri = "s3://my-bucket/";

        S3ObjectInfo s3ObjectInfo = new S3ObjectInfo(s3Uri);

        assertEquals("", s3ObjectInfo.key());
        assertEquals("s3://my-bucket/new-suffix", s3ObjectInfo.withNewKeySuffix("new-suffix").toString());
        assertEquals("new-suffix", s3ObjectInfo.withNewKeySuffix("new-suffix").key());
        assertEquals("/123", s3ObjectInfo.replaceOrAppendKey("_COMPLETION_ID_", "123").key());
        assertEquals("/123.json", s3ObjectInfo.replaceOrAppendKey("_COMPLETION_ID_", "123", "123.json").key());
    }
}