/*
Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.metadata;

import com.amazonaws.services.neptune.util.CSVUtils;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;

import java.util.function.Supplier;

import static org.junit.Assert.*;

public class EdgeMetadataTest {

    private static final Supplier<String> ID_GENERATOR = () -> "edge-id";

    private EdgeMetadata createEdgeMetadata(String columnHeaders) {
        return EdgeMetadata.parse(
                CSVUtils.firstRecord(columnHeaders),
                ID_GENERATOR,
                new PropertyValueParser(MultiValuedRelationshipPropertyPolicy.LeaveAsString, "", false));
    }

    @Test
    public void shouldParseEdgeHeadersFromColumnHeaders() {
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        String expectedHeaders = "~id,~from,~to,~label,strength,timestamp";

        EdgeMetadata edgeMetadata = createEdgeMetadata(columnHeaders);
        assertEquals(expectedHeaders, String.join(",", edgeMetadata.headers()));
        assertEquals(6, edgeMetadata.firstColumnIndex());
    }

    @Test
    public void shouldIndicateWhetherARecordContainsAnEdge() {
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        EdgeMetadata edgeMetadata = createEdgeMetadata(columnHeaders);

        CSVRecord vertexRecord = CSVUtils.firstRecord("\"1\",\"Person\",\"address\",\"name\",\"1\",\"1\",,,,,");
        CSVRecord edgeRecord = CSVUtils.firstRecord(",,,,,,\"1\",\"2\",\"KNOWS\",\"10\",\"12345\"");

        assertFalse(edgeMetadata.isEdge(vertexRecord));
        assertTrue(edgeMetadata.isEdge(edgeRecord));
    }

    @Test
    public void shouldWrapRecordWithEdgeSpecificIterable() {
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        EdgeMetadata edgeMetadata = createEdgeMetadata(columnHeaders);

        CSVRecord record = CSVUtils.firstRecord(",,,,,,\"1\",\"2\",\"KNOWS\",\"10\",\"12345\"");
        Iterable<String> edge = edgeMetadata.toIterable(record);

        String expected = "edge-id,1,2,KNOWS,10,12345";

        assertEquals(expected, String.join(",", edge));
    }

    @Test
    public void shouldUpdateHeadersWithDataTypeInfo() {
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        PropertyValueParser propertyValueParser = new PropertyValueParser(MultiValuedRelationshipPropertyPolicy.LeaveAsString, "", true);

        EdgeMetadata edgeMetadata = EdgeMetadata.parse(
                CSVUtils.firstRecord(columnHeaders),
                ID_GENERATOR,
                propertyValueParser);

        CSVRecord record = CSVUtils.firstRecord(",,,,,,\"1\",\"2\",\"KNOWS\",\"10\",\"12345\"");
        edgeMetadata.toIterable(record).forEach(c -> {});

        String expected = "~id,~from,~to,~label,strength:byte,timestamp:short";

        assertEquals(expected, String.join(",", edgeMetadata.headers()));
    }
}