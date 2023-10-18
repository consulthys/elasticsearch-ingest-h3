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

import com.uber.h3core.H3Core;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.ingest.ConfigurationUtils.readIntProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;

public class H3Processor extends AbstractProcessor {

    public static final String TYPE = "h3";

    private final String field;
    private final String targetField;
    private final int from;
    private final int to;
    private final H3Core h3;

    public H3Processor(String tag, String description, String field, String targetField, int from, int to) throws IOException {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.from = from;
        this.to = to;

        try {
            this.h3 = AccessController.doPrivileged(
                (PrivilegedExceptionAction<H3Core>) (() -> H3Core.newInstance())
            );
        } catch (PrivilegedActionException pae) {
            throw new IOException("Could not instantiate the H3 engine");
        }
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        Object latitude = ingestDocument.getFieldValue(this.field + ".lat", Object.class);
        Object longitude = ingestDocument.getFieldValue(this.field + ".lon", Object.class);

        Double lat = Double.parseDouble(latitude.toString());
        Double lon = Double.parseDouble(longitude.toString());

        Map<String, String> h3Indexes = new HashMap<>();
        for (int res = this.from; res <= this.to; res++) {
            h3Indexes.put(String.valueOf(res), this.h3.latLngToCellAddress(lat, lon, res));
        }

        ingestDocument.setFieldValue(this.targetField, h3Indexes);

        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public String getField() {
        return this.field;
    }

    public String getTargetField() {
        return this.targetField;
    }

    public int getFrom() {
        return this.from;
    }

    public int getTo() {
        return this.to;
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public H3Processor create(Map<String, Processor.Factory> factories, String tag, String description, Map<String, Object> config) throws Exception {
            String field = readStringProperty(TYPE, tag, config, "field");
            String targetField = readStringProperty(TYPE, tag, config, "target_field", "h3");
            int from = readIntProperty(TYPE, tag, config, "from", 0);
            int to = readIntProperty(TYPE, tag, config, "to", 15);

            if (to < from) {
                throw new IllegalArgumentException("'from' must be smaller than 'to'");
            }
            if (from < 0 || from > 15) {
                throw new IllegalArgumentException("'from' must be between 0 and 15, but was " + from);
            }
            if (to < 0 || to > 15) {
                throw new IllegalArgumentException("'to' must be between 0 and 15, but was " + to);
            }

            return new H3Processor(tag, description, field, targetField, from, to);
        }
    }
}
