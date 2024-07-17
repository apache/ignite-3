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

package org.apache.ignite.internal.sql.engine.externalize;

import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.lang.IgniteInternalException;

/**
 * Callback for a relational expression to dump itself as JSON.
 *
 * @see RelJsonReader
 */
public class RelJsonWriter implements RelWriter {
    private static final boolean PRETTY_PRINT = false;
    // TODO: IgniteSystemProperties.getBoolean("IGNITE_CALCITE_REL_JSON_PRETTY_PRINT", false);

    private final RelJson relJson;

    private final List<Object> relList = new ArrayList<>();

    private final Map<RelNode, String> relIdMap = new IdentityHashMap<>();

    private final boolean pretty;

    private String previousId;

    private List<Pair<String, Object>> items = new ArrayList<>();

    /**
     * Write to json string.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static String toJson(RelNode rel) {
        RelJsonWriter writer = new RelJsonWriter(PRETTY_PRINT);
        rel.explain(writer);

        return writer.asString();
    }

    /** Converts the given {@link RexNode} to json. */
    public static String toExprJson(RexNode node) {
        ObjectMapper mapper = new ObjectMapper();

        try {
            RelJson relJson = new RelJson();

            Object map = relJson.toJson(node);

            return mapper.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            throw new IgniteInternalException(INTERNAL_ERR, "RelJson expression serialization error", e);
        }
    }

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public RelJsonWriter(boolean pretty) {
        this.pretty = pretty;

        relJson = new RelJson();
    }

    /** {@inheritDoc} */
    @Override
    public final void explain(RelNode rel, List<Pair<String, Object>> valueList) {
        explain_(rel, valueList);
    }

    /** {@inheritDoc} */
    @Override
    public SqlExplainLevel getDetailLevel() {
        return SqlExplainLevel.ALL_ATTRIBUTES;
    }

    /** {@inheritDoc} */
    @Override
    public RelWriter item(String term, Object value) {
        items.add(Pair.of(term, value));
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public RelWriter done(RelNode node) {
        List<Pair<String, Object>> current0 = items;
        items = new ArrayList<>();
        explain_(node, current0);
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public boolean nest() {
        return true;
    }

    /**
     * AsString.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public String asString() {
        try {
            StringWriter writer = new StringWriter();
            ObjectMapper mapper = new ObjectMapper();

            ObjectWriter writer0 = pretty
                    ? mapper.writer(new DefaultPrettyPrinter())
                    : mapper.writer();

            writer0
                    .withRootName("rels")
                    .writeValue(writer, relList);

            return writer.toString();
        } catch (IOException e) {
            throw new IgniteInternalException(INTERNAL_ERR, "RelJson serialization error", e);
        }
    }

    private void explain_(RelNode rel, List<Pair<String, Object>> values) {
        final Map<String, Object> map = relJson.map();

        map.put("id", null); // ensure that id is the first attribute
        map.put("relOp", relJson.classToTypeName(rel.getClass()));

        for (Pair<String, Object> value : values) {
            if (value.right instanceof RelNode) {
                continue;
            }

            map.put(value.left, relJson.toJson(value.right));
        }
        // omit 'inputs: ["3"]' if "3" is the preceding rel
        final List<Object> list = explainInputs(rel.getInputs());
        if (list.size() != 1 || !list.get(0).equals(previousId)) {
            map.put("inputs", list);
        }

        final String id = Integer.toString(relIdMap.size());
        relIdMap.put(rel, id);
        map.put("id", id);

        relList.add(map);
        previousId = id;
    }

    private List<Object> explainInputs(List<RelNode> inputs) {
        final List<Object> list = relJson.list();
        for (RelNode input : inputs) {
            String id = relIdMap.get(input);
            if (id == null) {
                input.explain(this);
                id = previousId;
            }
            list.add(id);
        }
        return list;
    }
}
