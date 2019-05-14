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

package com.amazonaws.services.neptune.propertygraph.metadata;

import com.amazonaws.services.neptune.io.Status;
import com.amazonaws.services.neptune.propertygraph.*;
import com.amazonaws.services.neptune.propertygraph.io.ExportPropertyGraphTask;
import com.amazonaws.services.neptune.propertygraph.io.GraphElementHandler;
import com.amazonaws.services.neptune.propertygraph.io.TargetConfig;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.Collection;
import java.util.Map;

public class MetadataSpecification<T> {
    private final MetadataType<T> metadataType;
    private final LabelsFilter labelsFilter;
    private final boolean tokensOnly;

    public MetadataSpecification(MetadataType<T> metadataType, LabelsFilter labelsFilter, boolean tokensOnly) {
        this.metadataType = metadataType;
        this.labelsFilter = labelsFilter;
        this.tokensOnly = tokensOnly;
    }

    public void scan(PropertiesMetadataCollection metadataCollection, GraphTraversalSource g) {
        if (tokensOnly) {
            return;
        }

        GraphClient<T> graphClient = metadataType.graphClient(g, tokensOnly);

        graphClient.queryForMetadata(
                new Handler(metadataType, metadataCollection),
                Range.ALL,
                labelsFilter);
    }

    public void sample(PropertiesMetadataCollection metadataCollection, GraphTraversalSource g, long sampleSize) {
        if (tokensOnly) {
            return;
        }

        GraphClient<T> graphClient = metadataType.graphClient(g, tokensOnly);
        Collection<String> labels = labelsFilter.resolveLabels(graphClient);

        for (String label : labels) {
            graphClient.queryForMetadata(
                    new Handler(metadataType, metadataCollection),
                    new Range(0, sampleSize),
                    SpecifiedLabels.forLabels(label));
        }
    }

    public String description() {
        return metadataType.name();
    }

    public RangeFactory createRangeFactory(GraphTraversalSource g, ConcurrencyConfig concurrencyConfig) {
        return RangeFactory.create(metadataType.graphClient(g, tokensOnly), labelsFilter, concurrencyConfig);
    }

    public ExportPropertyGraphTask<T> createExportTask(PropertiesMetadataCollection metadataCollection,
                                                       GraphTraversalSource g,
                                                       TargetConfig targetConfig,
                                                       RangeFactory rangeFactory,
                                                       Status status,
                                                       int index) {
        return new ExportPropertyGraphTask<>(
                metadataCollection.propertyMetadataFor(metadataType),
                labelsFilter,
                metadataType.graphClient(g, tokensOnly),
                metadataType.writerFactory(),
                targetConfig,
                rangeFactory,
                status,
                index
        );
    }

    private static class Handler implements GraphElementHandler<Map<?, Object>> {

        private final MetadataType metadataType;
        private final PropertiesMetadataCollection metadataCollection;
        private final Status status = new Status();

        private Handler(MetadataType metadataType, PropertiesMetadataCollection metadataCollection) {
            this.metadataType = metadataType;
            this.metadataCollection = metadataCollection;
        }

        @Override
        public void handle(Map<?, Object> properties, boolean allowTokens) {
            status.update();
            metadataCollection.update(metadataType, properties, allowTokens);
        }

        @Override
        public void close() throws Exception {
            // Do nothing
        }
    }
}
