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
import com.amazonaws.services.neptune.propertygraph.EdgeLabelStrategy;
import com.amazonaws.services.neptune.rdf.RdfExportScope;
import com.amazonaws.services.neptune.rdf.io.RdfExportFormat;

public enum DataModel {
    PropertyGraph {
        @Override
        void updateArgsBeforeExport(Args args) {
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
    },
    RDF {
        @Override
        void updateArgsBeforeExport(Args args) {
            args.removeOptions("--format");
            args.addOption("--format", RdfExportFormat.ntriples.name());

            args.removeOptions("--rdf-export-scope");
            args.addOption("--rdf-export-scope", RdfExportScope.edges.name());

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
    };

    abstract void updateArgsBeforeExport(Args args);

    public abstract String nodeTypeName();

    public abstract String nodeAttributeNameSingular();

    public abstract String nodeAttributeNamePlural();
}
