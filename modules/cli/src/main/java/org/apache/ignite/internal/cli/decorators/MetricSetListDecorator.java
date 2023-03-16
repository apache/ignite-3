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

import java.util.List;
import java.util.StringJoiner;
import org.apache.ignite.internal.cli.core.decorator.Decorator;
import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;
import org.apache.ignite.rest.client.model.Metric;
import org.apache.ignite.rest.client.model.MetricSet;

/** Decorator for printing list of {@link MetricSet}. */
public class MetricSetListDecorator implements Decorator<List<MetricSet>, TerminalOutput> {
    @Override
    public TerminalOutput decorate(List<MetricSet> data) {
        return () -> {
            StringJoiner joiner = new StringJoiner(System.lineSeparator());

            joiner.add("Metric sets:");
            for (MetricSet metricSet : data) {
                joiner.add("  " + metricSet.getName());
                for (Metric metric : metricSet.getMetrics()) {
                    String desc = metric.getDesc();
                    joiner.add("    " + metric.getName() + (desc != null ? " - " + desc : ""));
                }
            }

            return joiner.toString();
        };
    }
}
