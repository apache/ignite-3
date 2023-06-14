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

package org.apache.ignite.internal.cli.util;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class PlainTableRendererTest {

    @Test
    void testRender() {
        String[] header = {"id", "name", "address"};
        Object[] row1 = {1, "John", null};
        Object[] row2 = {2, "Jessica", "any address"};
        Object[][] content = {row1, row2};
        String render = PlainTableRenderer.render(header, content);
        String[] renderedRows = render.split(System.lineSeparator());
        assertAll(
                () -> assertEquals(3, renderedRows.length),
                () -> assertEquals(3, renderedRows[0].split("\t").length),
                () -> assertEquals(3, renderedRows[1].split("\t").length),
                () -> assertEquals(3, renderedRows[2].split("\t").length)
        );
    }
}
