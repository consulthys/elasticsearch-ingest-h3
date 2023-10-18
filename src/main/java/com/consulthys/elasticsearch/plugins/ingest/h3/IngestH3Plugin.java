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

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class IngestH3Plugin extends Plugin implements IngestPlugin {

    //public static final Setting<String> YOUR_SETTING =
    //        new Setting<>("ingest.h3.setting", "foo", (value) -> value, Setting.Property.NodeScope);

    @Override
    public List<Setting<?>> getSettings() {
        return Collections.emptyList();
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Map.<String, Processor.Factory>of(H3Processor.TYPE, new H3Processor.Factory());
    }

}
