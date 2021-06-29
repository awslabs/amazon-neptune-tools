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

import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import org.apache.commons.lang.StringUtils;

public class TransferManagerWrapper implements AutoCloseable {

    private final TransferManager transferManager;

    public TransferManagerWrapper(String s3Region) {

        AmazonS3ClientBuilder amazonS3ClientBuilder = AmazonS3ClientBuilder.standard();

        if (StringUtils.isNotEmpty(s3Region)) {
            amazonS3ClientBuilder = amazonS3ClientBuilder.withRegion(s3Region);
        }

        transferManager = TransferManagerBuilder.standard()
                .withS3Client(amazonS3ClientBuilder.build())
                .build();
    }

    public TransferManager get() {
        return transferManager;
    }

    @Override
    public void close() {
        transferManager.shutdownNow();
    }
}
