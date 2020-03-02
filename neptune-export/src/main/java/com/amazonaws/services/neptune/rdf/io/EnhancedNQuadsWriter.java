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

import com.amazonaws.services.neptune.io.OutputWriter;
import com.amazonaws.services.neptune.io.Status;
import com.amazonaws.services.neptune.rdf.Prefixes;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.nquads.NQuadsWriter;

public class EnhancedNQuadsWriter extends NQuadsWriter {


    private final OutputWriter writer;
    private final Prefixes prefixes;
    private final Status status = new Status();

    public EnhancedNQuadsWriter(OutputWriter writer, Prefixes prefixes) {
        super(writer.writer());
        this.writer = writer;
        this.prefixes = prefixes;
    }

    @Override
    public void handleStatement(Statement statement) throws RDFHandlerException {
        prefixes.parse(statement.getSubject().stringValue(), this);
        prefixes.parse(statement.getPredicate().toString(), this);
        prefixes.parse(statement.getObject().stringValue(), this);

        Resource context = statement.getContext();
        if (context != null){
            prefixes.parse(context.stringValue(), this);
        }

        writer.startCommit();
        super.handleStatement(statement);
        writer.endCommit();

        status.update();
    }

    @Override
    public void handleNamespace(String prefix, String name) {
        writer.startCommit();
        super.handleNamespace(prefix, name);
        writer.endCommit();
    }
}
