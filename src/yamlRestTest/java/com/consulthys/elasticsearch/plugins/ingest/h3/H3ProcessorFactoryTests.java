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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class H3ProcessorFactoryTests extends ESTestCase {

    public void testCreate() throws Exception {
        H3Processor.Factory factory = new H3Processor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "location");
        String processorTag = randomAlphaOfLength(10);
        H3Processor h3Processor = factory.create(null, processorTag, null, config);
        assertThat(h3Processor.getTag(), is(processorTag));
        assertThat(h3Processor.getField(), is("location"));
        assertThat(h3Processor.getTargetField(), is("h3"));
        assertThat(h3Processor.getFrom(), is(0));
        assertThat(h3Processor.getTo(), is(15));
    }

    public void testCreateNoFieldPresent() throws Exception {
        H3Processor.Factory factory = new H3Processor.Factory();
        Map<String, Object> config = new HashMap<>();
        try {
            factory.create(null,null, null, config);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("[field] required property is missing"));
        }
    }

    public void testCreateFromBiggerThanTo() throws Exception {
        H3Processor.Factory factory = new H3Processor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "location");
        config.put("from", "10");
        config.put("to", "5");
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("'from' must be smaller than 'to'"));
        }
    }

    public void testCreateFromOutOfRange1() throws Exception {
        H3Processor.Factory factory = new H3Processor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "location");
        config.put("from", "-1");
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("'from' must be between 0 and 15, but was -1"));
        }
    }

    public void testCreateFromOutOfRange2() throws Exception {
        H3Processor.Factory factory = new H3Processor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "location");
        config.put("from", "18");
        config.put("to", "20");
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("'from' must be between 0 and 15, but was 18"));
        }
    }
    
    public void testCreateToOutOfRange() throws Exception {
        H3Processor.Factory factory = new H3Processor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "location");
        config.put("from", "13");
        config.put("to", "20");
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("'to' must be between 0 and 15, but was 20"));
        }
    }
}

