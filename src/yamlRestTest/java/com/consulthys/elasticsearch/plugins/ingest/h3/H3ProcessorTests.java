/*
 * Copyright [2018] [@consulthys]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.consulthys.elasticsearch.plugins.ingest.h3;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;

public class H3ProcessorTests extends ESTestCase {

    public void testH3WorksWithDefaultParams() throws Exception {
        Map<String, Object> document = new HashMap<>();
        Map<String, Double> location = new HashMap<>();
        location.put("lat", 0.0);
        location.put("lon", 0.0);
        document.put("location", location);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        H3Processor processor = new H3Processor(randomAlphaOfLength(10), null, "location", "h3", 0, 15);
        Map<String, Object> data = processor.execute(ingestDocument).getSourceAndMetadata();

        assertThat(data, hasKey("h3"));
        assertThat(((Map)data.get("h3")).get("0"), is("8075fffffffffff"));
        assertThat(((Map)data.get("h3")).get("1"), is("81757ffffffffff"));
        assertThat(((Map)data.get("h3")).get("2"), is("82754ffffffffff"));
        assertThat(((Map)data.get("h3")).get("3"), is("83754efffffffff"));
        assertThat(((Map)data.get("h3")).get("4"), is("84754a9ffffffff"));
        assertThat(((Map)data.get("h3")).get("5"), is("85754e67fffffff"));
        assertThat(((Map)data.get("h3")).get("6"), is("86754e64fffffff"));
        assertThat(((Map)data.get("h3")).get("7"), is("87754e64dffffff"));
        assertThat(((Map)data.get("h3")).get("8"), is("88754e6499fffff"));
        assertThat(((Map)data.get("h3")).get("9"), is("89754e64993ffff"));
        assertThat(((Map)data.get("h3")).get("10"), is("8a754e64992ffff"));
        assertThat(((Map)data.get("h3")).get("11"), is("8b754e649929fff"));
        assertThat(((Map)data.get("h3")).get("12"), is("8c754e649929dff"));
        assertThat(((Map)data.get("h3")).get("13"), is("8d754e64992d6ff"));
        assertThat(((Map)data.get("h3")).get("14"), is("8e754e64992d6df"));
        assertThat(((Map)data.get("h3")).get("15"), is("8f754e64992d6d8"));
        assertNull(((Map)data.get("h3")).get("16"));
    }

    public void testH3WorksWithStringLocation() throws Exception {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> location = new HashMap<>();
        location.put("lat", "0.0");
        location.put("lon", "0.0");
        document.put("location", location);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        H3Processor processor = new H3Processor(randomAlphaOfLength(10), null, "location", "h3", 0, 15);
        Map<String, Object> data = processor.execute(ingestDocument).getSourceAndMetadata();

        assertThat(data, hasKey("h3"));
        assertThat(((Map)data.get("h3")).get("0"), is("8075fffffffffff"));
        assertThat(((Map)data.get("h3")).get("1"), is("81757ffffffffff"));
        assertThat(((Map)data.get("h3")).get("2"), is("82754ffffffffff"));
        assertThat(((Map)data.get("h3")).get("3"), is("83754efffffffff"));
        assertThat(((Map)data.get("h3")).get("4"), is("84754a9ffffffff"));
        assertThat(((Map)data.get("h3")).get("5"), is("85754e67fffffff"));
        assertThat(((Map)data.get("h3")).get("6"), is("86754e64fffffff"));
        assertThat(((Map)data.get("h3")).get("7"), is("87754e64dffffff"));
        assertThat(((Map)data.get("h3")).get("8"), is("88754e6499fffff"));
        assertThat(((Map)data.get("h3")).get("9"), is("89754e64993ffff"));
        assertThat(((Map)data.get("h3")).get("10"), is("8a754e64992ffff"));
        assertThat(((Map)data.get("h3")).get("11"), is("8b754e649929fff"));
        assertThat(((Map)data.get("h3")).get("12"), is("8c754e649929dff"));
        assertThat(((Map)data.get("h3")).get("13"), is("8d754e64992d6ff"));
        assertThat(((Map)data.get("h3")).get("14"), is("8e754e64992d6df"));
        assertThat(((Map)data.get("h3")).get("15"), is("8f754e64992d6d8"));
        assertNull(((Map)data.get("h3")).get("16"));
    }

    public void testH3WorksWithSpecifiedResolution() throws Exception {
        Map<String, Object> document = new HashMap<>();
        Map<String, Double> location = new HashMap<>();
        location.put("lat", 0.0);
        location.put("lon", 0.0);
        document.put("location", location);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        H3Processor processor = new H3Processor(randomAlphaOfLength(10),null, "location", "h3", 8, 10);
        Map<String, Object> data = processor.execute(ingestDocument).getSourceAndMetadata();

        assertThat(data, hasKey("h3"));
        assertNull(((Map)data.get("h3")).get("0"));
        assertNull(((Map)data.get("h3")).get("1"));
        assertNull(((Map)data.get("h3")).get("2"));
        assertNull(((Map)data.get("h3")).get("3"));
        assertNull(((Map)data.get("h3")).get("4"));
        assertNull(((Map)data.get("h3")).get("5"));
        assertNull(((Map)data.get("h3")).get("6"));
        assertNull(((Map)data.get("h3")).get("7"));
        assertThat(((Map)data.get("h3")).get("8"), is("88754e6499fffff"));
        assertThat(((Map)data.get("h3")).get("9"), is("89754e64993ffff"));
        assertThat(((Map)data.get("h3")).get("10"), is("8a754e64992ffff"));
        assertNull(((Map)data.get("h3")).get("11"));
        assertNull(((Map)data.get("h3")).get("12"));
        assertNull(((Map)data.get("h3")).get("13"));
        assertNull(((Map)data.get("h3")).get("14"));
        assertNull(((Map)data.get("h3")).get("15"));
        assertNull(((Map)data.get("h3")).get("16"));
    }

    public void testH3ComplainsAboutFromBiggerThanTo() throws Exception {
        Map<String, Object> document = new HashMap<>();
        Map<String, Double> location = new HashMap<>();
        location.put("lat", 0.0);
        location.put("lon", 0.0);
        document.put("location", location);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        H3Processor processor = new H3Processor(randomAlphaOfLength(10), null, "location", "h3", 10, 8);

        Map<String, Object> data = processor.execute(ingestDocument).getSourceAndMetadata();
        assertThat(((Map)data.get("h3")).size(), is(0));
    }

    public void testH3ComplainsAboutFromOutOfRange() throws Exception {
        Map<String, Object> document = new HashMap<>();
        Map<String, Double> location = new HashMap<>();
        location.put("lat", 0.0);
        location.put("lon", 0.0);
        document.put("location", location);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        H3Processor processor = new H3Processor(randomAlphaOfLength(10), null, "location", "h3", -1, 8);

        try {
            processor.execute(ingestDocument);
            fail("It should have failed because 'from' is out of range");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), is("resolution -1 is out of range (must be 0 <= res <= 15)"));
        }

        processor = new H3Processor(randomAlphaOfLength(10), null, "location", "h3", 16, 18);

        try {
            Map<String, Object> data = processor.execute(ingestDocument).getSourceAndMetadata();
            fail("It should have failed because 'from' is out of range");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), is("resolution 16 is out of range (must be 0 <= res <= 15)"));
        }
    }

    public void testH3ComplainsAboutToOutOfRange() throws Exception {
        Map<String, Object> document = new HashMap<>();
        Map<String, Double> location = new HashMap<>();
        location.put("lat", 0.0);
        location.put("lon", 0.0);
        document.put("location", location);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        H3Processor processor = new H3Processor(randomAlphaOfLength(10),null, "location", "h3", -10, -1);

        try {
            processor.execute(ingestDocument);
            fail("It should have failed because 'to' is out of range");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), is("resolution -10 is out of range (must be 0 <= res <= 15)"));
        }

        processor = new H3Processor(randomAlphaOfLength(10), null, "location", "h3", 16, 18);

        try {
            processor.execute(ingestDocument);
            fail("It should have failed because 'to' is out of range");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), is("resolution 16 is out of range (must be 0 <= res <= 15)"));
        }
    }
}

