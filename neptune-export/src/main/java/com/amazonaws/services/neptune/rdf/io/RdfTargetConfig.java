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

import com.amazonaws.services.neptune.io.Directories;
import com.amazonaws.services.neptune.io.KinesisConfig;
import com.amazonaws.services.neptune.io.OutputWriter;
import com.amazonaws.services.neptune.io.Target;
import com.amazonaws.services.neptune.rdf.Prefixes;
import org.eclipse.rdf4j.rio.RDFWriter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Supplier;

public class RdfTargetConfig {

    private final Directories directories;
    private final Target output;
    private final KinesisConfig kinesisConfig;
    private final RdfExportFormat format;

    public RdfTargetConfig(Directories directories, KinesisConfig kinesisConfig, Target output, RdfExportFormat format) {
        this.directories = directories;
        this.output = output;
        this.kinesisConfig = kinesisConfig;
        this.format = format;
    }

    public OutputWriter createOutputWriter() throws IOException {
        return output.createOutputWriter(
                () -> directories.createStatementsFilePath("statements", 0, format),
                kinesisConfig);
    }

    public RDFWriter createRDFWriter(OutputWriter outputWriter) {
        return format.createWriter(outputWriter, new Prefixes());
    }

    public RdfExportFormat format() {
        return format;
    }
}
