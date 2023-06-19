/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.cli.decorators;

import com.jakewharton.fliptables.FlipTable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.cli.call.unit.UnitStatusRecord;
import org.apache.ignite.internal.cli.core.decorator.Decorator;
import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;
import org.apache.ignite.internal.cli.util.PlainTableRenderer;
import org.apache.ignite.rest.client.model.DeploymentStatus;

/** Decorates list of units as a table. */
public class UnitListDecorator implements Decorator<List<UnitStatusRecord>, TerminalOutput> {

    private static final String[] HEADERS = {"id", "version", "status"};
    private final boolean plain;

    public UnitListDecorator(boolean plain) {
        this.plain = plain;
    }

    @Override
    public TerminalOutput decorate(List<UnitStatusRecord> data) {
        if (plain) {
            return () -> PlainTableRenderer.render(HEADERS, toContent(data));
        } else {
            return () -> FlipTable.of(HEADERS, toContent(data));
        }
    }

    private static String[][] toContent(List<UnitStatusRecord> data) {
        return data.stream().flatMap(UnitListDecorator::unfoldRecordWithVersions).toArray(String[][]::new);
    }

    private static Stream<String[]> unfoldRecordWithVersions(UnitStatusRecord record) {
        Map<Version, DeploymentStatus> map = record.versionToStatus();
        Entry<Version, DeploymentStatus> max = map.entrySet().stream().max(Entry.comparingByKey()).orElse(null);
        if (max == null) {
            return Stream.empty();
        }
        return map.entrySet().stream().map(entry -> {
            Version key = entry.getKey();
            String version = Objects.equals(key, max.getKey()) ? "*" + key.render() : key.render();

            return new String[]{
                    record.id(),
                    version,
                    entry.getValue().getValue()
            };
        });
    }
}
