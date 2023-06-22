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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.internal.cli.core.decorator.Decorator;
import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;
import org.apache.ignite.internal.cli.util.PlainTableRenderer;
import org.apache.ignite.rest.client.model.UnitStatus;
import org.apache.ignite.rest.client.model.UnitVersionStatus;

/** Decorates list of units as a table. */
public class UnitListDecorator implements Decorator<List<UnitStatus>, TerminalOutput> {

    private static final String[] HEADERS = {"id", "version", "status"};
    private final boolean plain;

    public UnitListDecorator(boolean plain) {
        this.plain = plain;
    }

    @Override
    public TerminalOutput decorate(List<UnitStatus> data) {
        if (plain) {
            return () -> PlainTableRenderer.render(HEADERS, toContent(data));
        } else {
            return () -> FlipTable.of(HEADERS, toContent(data));
        }
    }

    private static String[][] toContent(List<UnitStatus> data) {
        return data.stream().flatMap(UnitListDecorator::unfoldRecordWithVersions).toArray(String[][]::new);
    }

    private static Stream<String[]> unfoldRecordWithVersions(UnitStatus status) {
        List<String[]> result = new ArrayList<>();
        List<UnitVersionStatus> versionStatuses = status.getVersionToStatus();
        for (int i = 0, size = versionStatuses.size(); i < size; i++) {
            UnitVersionStatus entry = versionStatuses.get(i);
            result.add(new String[]{
                    status.getId(),
                    (i == size - 1 ? "*" : "") + entry.getVersion(),
                    entry.getStatus().getValue()
            });
        }
        return result.stream();
    }
}
