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
import java.util.stream.Stream;
import org.apache.ignite.internal.cli.call.unit.UnitStatusRecord;
import org.apache.ignite.internal.cli.core.decorator.Decorator;
import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;

/** Decorates list of units as a table. */
public class UnitListDecorator implements Decorator<List<UnitStatusRecord>, TerminalOutput> {

    private static final String[] HEADERS = {"id", "version", "status"};

    private String[][] toContent(List<UnitStatusRecord> data) {
        return data.stream().flatMap(this::unfoldRecordWithVersions).toArray(String[][]::new);
    }

    @Override
    public TerminalOutput decorate(List<UnitStatusRecord> data) {
        return () -> FlipTable.of(HEADERS, toContent(data));
    }

    private Stream<String[]> unfoldRecordWithVersions(UnitStatusRecord record) {
        Map<String, List<String>> map = record.versionToConsistentIds();
        return map.keySet().stream().map(version -> new String[]{
                record.id(),
                version,
                "TBD"
        });
    }
}
