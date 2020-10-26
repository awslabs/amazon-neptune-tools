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

import com.amazonaws.services.neptune.propertygraph.schema.PropertySchema;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface PropertyGraphPrinter extends AutoCloseable {

    String outputId();

    void printHeaderMandatoryColumns(String... columns);

    void printHeaderRemainingColumns(Collection<PropertySchema> remainingColumns);

    void printProperties(Map<?, ?> properties) throws IOException;

    void printProperties(Map<?, ?> properties, boolean applyFormatting) throws IOException;

    void printProperties(String id, String streamOperation, Map<?, ?> properties) throws IOException;

    void printEdge(String id, String label, String from, String to) throws IOException;

    void printEdge(String id, String label, String from, String to, Collection<String> fromLabels, Collection<String> toLabels) throws IOException;

    void printNode(String id, List<String> labels) throws IOException;

    void printStartRow() throws IOException;

    void printEndRow() throws IOException;
}
