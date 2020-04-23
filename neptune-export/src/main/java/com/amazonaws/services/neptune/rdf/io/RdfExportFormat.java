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

package com.amazonaws.services.neptune.rdf.io;

import com.amazonaws.services.neptune.io.FileExtension;
import com.amazonaws.services.neptune.io.OutputWriter;
import com.amazonaws.services.neptune.rdf.Prefixes;
import org.eclipse.rdf4j.rio.RDFWriter;

public enum RdfExportFormat implements FileExtension {
    turtle {
        @Override
        RDFWriter createWriter(OutputWriter writer, Prefixes prefixes) {
            return new EnhancedTurtleWriter(writer, prefixes);
        }

        @Override
        public String suffix() {
            return "ttl";
        }

        @Override
        public String description() {
            return "Turtle";
        }

    },
    nquads {
        @Override
        RDFWriter createWriter(OutputWriter writer, Prefixes prefixes) {
            return new EnhancedNQuadsWriter(writer, prefixes);
        }


        @Override
        public String suffix() {
            return "nq";
        }

        @Override
        public String description() {
            return "NQUADS";
        }


    },
    neptuneStreamsJson {
        @Override
        RDFWriter createWriter(OutputWriter writer, Prefixes prefixes) {
            return new NeptuneStreamsJsonNQuadsWriter(writer);
        }

        @Override
        public String suffix() {
            return "json";
        }

        @Override
        public String description() {
            return "JSON (Neptune Streams format)";
        }

    };

    abstract RDFWriter createWriter(OutputWriter writer, Prefixes prefixes);

    public abstract String description();

}
