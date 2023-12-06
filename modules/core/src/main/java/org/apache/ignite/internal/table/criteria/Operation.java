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

package org.apache.ignite.internal.table.criteria;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.ignite.table.criteria.Criteria;
import org.jetbrains.annotations.Nullable;

/**
 * {@code Operation} represents an operation with operator and arguments.
 */
public class Operation implements Criteria, CriteriaElement {
    private static final Pattern elementPattern = Pattern.compile("\\{(\\d+)\\}");
    private final List<CriteriaElement> elements;

    private Operation(List<CriteriaElement> elements) {
        this.elements = elements;
    }

    /** {@inheritDoc} */
    @Override
    public <C> void accept(CriteriaVisitor<C> v, @Nullable C context) {
        for (var element : elements) {
            element.accept(v, null);
        }
    }

    static Operation create(String template, List<CriteriaElement> arguments) {
        int end = 0;
        var elements = new ArrayList<CriteriaElement>();

        var matcher = elementPattern.matcher(template);

        while (matcher.find()) {
            if (matcher.start() > end) {
                elements.add(new StaticText(template.substring(end, matcher.start())));
            }

            int index = Integer.parseInt(matcher.group(1));

            elements.add(arguments.get(index));

            end = matcher.end();
        }

        if (end < template.length()) {
            elements.add(new StaticText(template.substring(end)));
        }

        return new Operation(elements);
    }
}
