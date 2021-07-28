package com.amazonaws.services.neptune.io;

import com.amazonaws.services.neptune.profiles.neptune_ml.JsonFromResource;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class RecordSplitterTest {

    private final LargeStreamRecordHandlingStrategy STRATEGY = LargeStreamRecordHandlingStrategy.splitAndShred;

    @Test
    public void shouldSplitStringByLength(){

        String s = "abcdefghijklmno";

        assertStringCollections(Collections.singletonList("abcdefghijklmno"), RecordSplitter.splitByLength(s, 15));
        assertStringCollections(Collections.singletonList("abcdefghijklmno"), RecordSplitter.splitByLength(s, 20));
        assertStringCollections(Arrays.asList("abcde", "fghij", "klmno"), RecordSplitter.splitByLength(s, 5));
        assertStringCollections(Arrays.asList("abcdef", "ghijkl", "mno"), RecordSplitter.splitByLength(s, 6));
    }

    @Test
    public void shouldSplitStringAttemptToSplitOnWordBoundary(){

        String s = " abc defghij klmno ";

        assertStringCollections(Collections.singletonList("abc defghij klmno"), RecordSplitter.splitByLength(s, 19, 4));
        assertStringCollections(Collections.singletonList("abc defghij klmno"), RecordSplitter.splitByLength(s, 24, 4));
        assertStringCollections(Arrays.asList("abc", "defgh", "ij", "klmno"), RecordSplitter.splitByLength(s, 5));
        assertStringCollections(Arrays.asList("abc", "defghi", "j", "klmno"), RecordSplitter.splitByLength(s, 6, 4));
    }

    @Test
    public void shouldSplitIntoIndividualNeptuneStreamsPGRecords() throws IOException {
        TestFixture testFixture = new TestFixture("t1.json", getClass());

        RecordSplitter recordSplitter = new RecordSplitter(160, STRATEGY);
        Collection<String> records = recordSplitter.split(testFixture.input());

        assertStringCollections(testFixture.expectedOutputs(), records);
    }

    @Test
    public void shouldSubdivideLongPGRecords() throws IOException {
        TestFixture testFixture = new TestFixture("t2.json", getClass());

        RecordSplitter recordSplitter = new RecordSplitter(160, STRATEGY);
        Collection<String> records = recordSplitter.split(testFixture.input());

        assertStringCollections(testFixture.expectedOutputs(), records);
    }

    @Test
    public void shouldSplitCsvIntoIndividualFields() throws IOException {
        TestFixture testFixture = new TestFixture("t3.json", getClass());

        RecordSplitter recordSplitter = new RecordSplitter(160, STRATEGY);
        Collection<String> records = recordSplitter.split(testFixture.input());

        assertStringCollections(testFixture.expectedOutputs(), records);
    }

    @Test
    public void shouldSubdivideLongCsvFields() throws IOException {
        TestFixture testFixture = new TestFixture("t4.json", getClass());

        RecordSplitter recordSplitter = new RecordSplitter(8, STRATEGY);
        Collection<String> records = recordSplitter.split(testFixture.input());

        assertStringCollections(testFixture.expectedOutputs(), records);
    }

    @Test
    public void shouldSplitIntoIndividualNeptuneStreamsRDFRecords() throws IOException {
        TestFixture testFixture = new TestFixture("t5.json", getClass());

        RecordSplitter recordSplitter = new RecordSplitter(160, STRATEGY);
        Collection<String> records = recordSplitter.split(testFixture.input());

        assertStringCollections(testFixture.expectedOutputs(), records);
    }

    @Test
    public void shouldSubdivideLongRDFRecords() throws IOException {
        TestFixture testFixture = new TestFixture("t6.json", getClass());

        RecordSplitter recordSplitter = new RecordSplitter(140, STRATEGY);
        Collection<String> records = recordSplitter.split(testFixture.input());

        assertStringCollections(testFixture.expectedOutputs(), records);
    }

    private void assertStringCollections(Collection<String> expectedOutputs, Collection<String> records) {

        String msg = String.format("Expected: %s\nActual: %s", expectedOutputs, records);

        assertEquals(msg, expectedOutputs.size(), records.size());
        Iterator<String> expectedIterator = expectedOutputs.iterator();
        Iterator<String> recordsIterator = records.iterator();

        while (expectedIterator.hasNext()) {
            String expected = expectedIterator.next();
            String actual = recordsIterator.next();
            assertEquals(expected, actual);
        }
    }

    private static class TestFixture {
        private final String input;
        private final Collection<String> expectedOutputs = new ArrayList<>();

        public TestFixture(String filename, Class<?> clazz) throws IOException {
            JsonNode json = JsonFromResource.get(filename, clazz);
            this.input = json.get("input").toString();
            ArrayNode output = (ArrayNode) json.get("output");
            for (JsonNode jsonNode : output) {
                expectedOutputs.add(jsonNode.toString());
            }
        }

        public String input() {
            return input;
        }

        public Collection<String> expectedOutputs() {
            return expectedOutputs;
        }
    }
}