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

package org.apache.ignite.internal.cli.core.call;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Test;

class SpinnerRendererTest {

    @Test
    void render() {
        SpinnerRenderer renderer = new SpinnerRenderer("prefix");

        assertThat(renderer.render(0), is(emptyString()));

        assertThat(renderer.render(11), is("prefix.  \b\b"));
        assertThat(renderer.render(11), is("prefix.. \b"));
        assertThat(renderer.render(11), is("prefix..."));
        assertThat(renderer.render(11), is("prefix.  \b\b"));

        assertThat(renderer.render(1), is("p"));
    }
}
