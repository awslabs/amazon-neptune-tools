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

package com.amazonaws.services.neptune.profiles.neptune_ml;

import com.amazonaws.services.neptune.export.Args;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.ParseProperty;
import com.amazonaws.services.neptune.profiles.neptune_ml.common.parsing.ParsingContext;
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.config.LabelConfigV2;
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.config.RdfTaskTypeV2;
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.config.TrainingDataWriterConfigV2;
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.parsing.ParseNodeTaskTypeV2;
import com.amazonaws.services.neptune.profiles.neptune_ml.v2.parsing.ParseRdfTaskType;
import com.amazonaws.services.neptune.propertygraph.EdgeLabelStrategy;
import com.amazonaws.services.neptune.propertygraph.Label;
import com.amazonaws.services.neptune.rdf.RdfExportScope;
import com.amazonaws.services.neptune.rdf.io.RdfExportFormat;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Collection;

public enum NeptuneMLSourceDataModel {
    PropertyGraph {
        @Override
        void updateArgsBeforeExport(Args args, Collection<TrainingDataWriterConfigV2> trainingJobWriterConfigCollection) {
            if (!args.contains("--exclude-type-definitions")) {
                args.addFlag("--exclude-type-definitions");
            }

            if (args.contains("--edge-label-strategy", EdgeLabelStrategy.edgeLabelsOnly.name())) {
                args.removeOptions("--edge-label-strategy");
            }

            if (!args.contains("--edge-label-strategy", EdgeLabelStrategy.edgeAndVertexLabels.name())) {
                args.addOption("--edge-label-strategy", EdgeLabelStrategy.edgeAndVertexLabels.name());
            }

            if (!args.contains("--merge-files")) {
                args.addFlag("--merge-files");
            }

            if (args.contains("export-pg") &&
                    args.containsAny("--config", "--filter", "-c", "--config-file", "--filter-config-file")) {
                args.replace("export-pg", "export-pg-from-config");
            }
        }

        @Override
        public String nodeTypeName() {
            return "Label";
        }

        @Override
        public String nodeAttributeNameSingular() {
            return "Property";
        }

        @Override
        public String nodeAttributeNamePlural() {
            return "Properties";
        }

        @Override
        public String parseTaskType(JsonNode json, ParsingContext propertyContext, Label nodeType, String property) {
            return new ParseNodeTaskTypeV2(json, propertyContext).parseTaskType().name();
        }

        @Override
        public String parseProperty(JsonNode json, ParsingContext propertyContext, Label nodeType) {
            return new ParseProperty(json, propertyContext.withLabel(nodeType), this).parseSingleProperty();
        }

        @Override
        public String labelFields() {
            return "'node' or 'edge'";
        }

        @Override
        public boolean isRdfLinkPrediction(JsonNode json) {
            return false;
        }
    },
    RDF {
        @Override
        void updateArgsBeforeExport(Args args, Collection<TrainingDataWriterConfigV2> trainingJobWriterConfigCollection) {
            args.removeOptions("--format");
            args.addOption("--format", RdfExportFormat.ntriples.name());

            boolean exportEdgesOnly = true;

            for (TrainingDataWriterConfigV2 trainingDataWriterConfigV2 : trainingJobWriterConfigCollection) {
                for (LabelConfigV2 labelConfig : trainingDataWriterConfigV2.nodeConfig().getAllClassificationSpecifications()) {
                    String taskType = labelConfig.taskType();
                    if (taskType.equals(RdfTaskTypeV2.classification.name()) || taskType.equals(RdfTaskTypeV2.regression.name())){
                        exportEdgesOnly = false;
                    }
                }
            }

            if (!args.contains("--rdf-export-scope") && exportEdgesOnly){
                args.addOption("--rdf-export-scope", RdfExportScope.edges.name());
            }
        }

        @Override
        public String nodeTypeName() {
            return "Class";
        }

        @Override
        public String nodeAttributeNameSingular() {
            return "Predicate";
        }

        @Override
        public String nodeAttributeNamePlural() {
            return "Predicates";
        }

        @Override
        public String parseTaskType(JsonNode json, ParsingContext propertyContext, Label nodeType, String property) {
            RdfTaskTypeV2 taskType = new ParseRdfTaskType(json, propertyContext).parseTaskType();
            taskType.validate(property, nodeType);
            return taskType.name();
        }

        @Override
        public String parseProperty(JsonNode json, ParsingContext propertyContext, Label nodeType) {
            return new ParseProperty(json, propertyContext.withLabel(nodeType), this).parseNullableSingleProperty();
        }

        @Override
        public String labelFields() {
            return "'node'";
        }

        @Override
        public boolean isRdfLinkPrediction(JsonNode json) {
            return parseTaskType(json, new ParsingContext("RDF target"), null, null).equals(RdfTaskTypeV2.link_prediction.name());
        }
    };

    abstract void updateArgsBeforeExport(Args args, Collection<TrainingDataWriterConfigV2> trainingJobWriterConfigCollection);

    public abstract String nodeTypeName();

    public abstract String nodeAttributeNameSingular();

    public abstract String nodeAttributeNamePlural();

    public abstract String parseTaskType(JsonNode json, ParsingContext propertyContext, Label nodeType, String property);

    public abstract String parseProperty(JsonNode json, ParsingContext propertyContext, Label nodeType);

    public abstract String labelFields();

    public abstract boolean isRdfLinkPrediction(JsonNode json);
}
