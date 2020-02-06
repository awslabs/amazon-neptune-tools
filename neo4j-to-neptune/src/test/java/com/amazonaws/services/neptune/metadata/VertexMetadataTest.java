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

import static org.junit.Assert.*;

public class VertexMetadataTest {

    private VertexMetadata createVertexMetadata(String columnHeaders) {
        return VertexMetadata.parse(
                CSVUtils.firstRecord(columnHeaders),
                new PropertyValueParser(MultiValuedNodePropertyPolicy.PutInSetIgnoringDuplicates, "", false));
    }

    @Test
    public void shouldParseVertexHeadersFromColumnHeaders() {
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        String expectedHeaders = "~id,~label,address,name,index,txid";

        VertexMetadata vertexMetadata = createVertexMetadata(columnHeaders);
        assertEquals(expectedHeaders, String.join(",", vertexMetadata.headers()));
        assertEquals(5, vertexMetadata.lastColumnIndex());
    }

    @Test
    public void shouldIndicateWhetherARecordContainsAVertex() {
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        VertexMetadata vertexMetadata = createVertexMetadata(columnHeaders);

        CSVRecord vertexRecord = CSVUtils.firstRecord("\"1\",\"Person\",\"address\",\"name\",\"1\",\"1\",,,,,");
        CSVRecord edgeRecord = CSVUtils.firstRecord(",,,,,,\"1\",\"2\",\"KNOWS\",\"10\",\"12345\"");

        assertTrue(vertexMetadata.isVertex(vertexRecord));
        assertFalse(vertexMetadata.isVertex(edgeRecord));
    }

    @Test
    public void shouldWrapRecordWithVertexSpecificIterable() {
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        VertexMetadata vertexMetadata = createVertexMetadata(columnHeaders);

        CSVRecord record = CSVUtils.firstRecord("\"1\",\"Person\",\"address\",\"name\",\"1\",\"1\",,,,,");
        Iterable<String> vertex = vertexMetadata.toIterable(record);

        String expected = "1,Person,address,name,1,1";

        assertEquals(expected, String.join(",", vertex));
    }

    @Test
    public void shouldRemoveLeadingColonFromLabel() {
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\",\"_start\",\"_end\",\"_type\",\"strength\",\"timestamp\"";
        VertexMetadata vertexMetadata = createVertexMetadata(columnHeaders);

        CSVRecord record = CSVUtils.firstRecord("\"1\",\":Person\",\"address\",\"name\",\"1\",\"1\",,,,,");
        Iterable<String> vertex = vertexMetadata.toIterable(record);

        String expected = "1,Person,address,name,1,1";

        assertEquals(expected, String.join(",", vertex));
    }

    @Test
    public void shouldCreateSemicolonDelimitedListOfLabelsForMultipleLabelsInSource(){
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\"";
        VertexMetadata vertexMetadata = createVertexMetadata(columnHeaders);

        CSVRecord record = CSVUtils.firstRecord("\"1\",\":Person:Admin\",\"address\",\"name\",\"1\",\"1\"");
        Iterable<String> vertex = vertexMetadata.toIterable(record);

        String expected = "1,Person;Admin,address,name,1,1";

        assertEquals(expected, String.join(",", vertex));
    }

    @Test
    public void shouldAllowEmptyLabels(){
        String columnHeaders = "\"_id\",\"_labels\",\"address\",\"name\",\"index\",\"txid\"";
        VertexMetadata vertexMetadata = createVertexMetadata(columnHeaders);

        CSVRecord record = CSVUtils.firstRecord("\"1\",\"\",\"address\",\"name\",\"1\",\"1\"");
        Iterable<String> vertex = vertexMetadata.toIterable(record);

        String expected = "1,,address,name,1,1";

        assertEquals(expected, String.join(",", vertex));
    }

}