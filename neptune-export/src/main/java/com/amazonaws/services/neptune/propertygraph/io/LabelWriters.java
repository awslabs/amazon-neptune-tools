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

package com.amazonaws.services.neptune.propertygraph.io;

import com.amazonaws.services.neptune.propertygraph.Label;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class LabelWriters<T extends Map<?, ?>> implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(LabelWriters.class);

    private final int maxFileDescriptorCount;

    private final AtomicInteger fileDescriptorCount;
    private final LinkedHashMap<Label, LabelWriter<T>> labelWriters = new LinkedHashMap<>(16, 0.75f, true);

    public LabelWriters(AtomicInteger fileDescriptorCount, int maxFileDescriptorCount) {
        this.fileDescriptorCount = fileDescriptorCount;
        this.maxFileDescriptorCount = maxFileDescriptorCount;
    }

    public boolean containsKey(Label label){
        return labelWriters.containsKey(label);
    }

    public void put(Label label, LabelWriter<T> labelWriter) throws Exception {

        if (fileDescriptorCount.get() > maxFileDescriptorCount && labelWriters.size() > 1){
            Label leastRecentlyAccessedLabel = labelWriters.keySet().iterator().next();
            LabelWriter<T> leastRecentlyAccessedLabelWriter = labelWriters.remove(leastRecentlyAccessedLabel);
            logger.info("Closing writer for label {} for output {} so as to conserve file descriptors", leastRecentlyAccessedLabel.labelsAsString(), leastRecentlyAccessedLabelWriter.outputId());
            leastRecentlyAccessedLabelWriter.close();
            fileDescriptorCount.decrementAndGet();
        }
        logger.debug("Adding writer for label {} for output {}", label.labelsAsString(), labelWriter.outputId());
        labelWriters.put(label, labelWriter);
        fileDescriptorCount.incrementAndGet();
    }

    @Override
    public void close() throws Exception {
        for (LabelWriter<T> writer : labelWriters.values()) {
            logger.info("Closing file: {}", writer.outputId());
            writer.close();
            fileDescriptorCount.decrementAndGet();
        }
    }

    public LabelWriter<T> get(Label label) {
        return labelWriters.get(label);
    }
}
