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

package org.apache.ignite.internal.cli.sql.table;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.ignite.internal.cli.decorators.TruncationConfig;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.jline.terminal.Size;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link AlternateScreenTableRenderer}.
 *
 * <p>Uses a dumb terminal to avoid JLine pump thread issues in tests.
 * ANSI sequences are still written to the output stream regardless of terminal type.
 */
class AlternateScreenTableRendererTest extends BaseIgniteAbstractTest {

    private ByteArrayOutputStream termOutput;
    private Terminal terminal;

    @BeforeEach
    void setUp() throws IOException {
        termOutput = new ByteArrayOutputStream();
        // Provide enough input bytes for terminal.reader().read() calls in finish().
        // DumbTerminal reads directly from the input stream without a pump thread.
        byte[] inputBytes = new byte[64];
        inputBytes[0] = '\n';
        ByteArrayInputStream termInput = new ByteArrayInputStream(inputBytes);
        terminal = TerminalBuilder.builder()
                .streams(termInput, termOutput)
                .system(false)
                .jna(false)
                .jansi(false)
                .jni(false)
                .type(Terminal.TYPE_DUMB)
                .size(new Size(120, 40))
                .build();
    }

    @AfterEach
    void tearDown() throws IOException {
        terminal.close();
    }

    @Test
    void enterEmitsAltScreenAndHeaderAndScrollRegion() {
        TruncationConfig config = new TruncationConfig(true, 50, 120);
        AlternateScreenTableRenderer renderer = new AlternateScreenTableRenderer(terminal, config);

        renderer.enter(new String[]{"ID", "NAME"}, new Object[][]{{"1", "Alice"}});

        String output = termOutput.toString();
        assertThat(output, containsString(AlternateScreenTableRenderer.ENTER_ALT_SCREEN));
        assertThat(output, containsString(AlternateScreenTableRenderer.CLEAR_SCREEN));
        assertThat(output, containsString(AlternateScreenTableRenderer.CURSOR_HOME));
        // Header line should contain column names
        assertThat(output, containsString("ID"));
        assertThat(output, containsString("NAME"));
        // Scroll region should be set based on terminal height.
        assertThat(output, containsString(AlternateScreenTableRenderer.setScrollRegion(
                AlternateScreenTableRenderer.HEADER_LINES + 1, terminal.getHeight())));
        // First page data should be rendered
        assertThat(output, containsString("Alice"));
        assertTrue(renderer.isActive());

        renderer.leave();
    }

    @Test
    void addPageWithoutWidthChangeAppendsRowsIncrementally() {
        TruncationConfig config = new TruncationConfig(true, 50, 120);
        AlternateScreenTableRenderer renderer = new AlternateScreenTableRenderer(terminal, config);

        renderer.enter(new String[]{"ID", "NAME"}, new Object[][]{{"1", "Alice"}});

        // Clear output to check only what addPage writes.
        termOutput.reset();
        renderer.addPage(new Object[][]{{"2", "Bob"}});

        String output = termOutput.toString();
        // No clear screen for incremental append.
        assertThat(output, not(containsString(AlternateScreenTableRenderer.CLEAR_SCREEN)));
        assertThat(output, containsString("Bob"));

        renderer.leave();
    }

    @Test
    void addPageWithWiderDataTriggersFullReRender() {
        TruncationConfig config = new TruncationConfig(true, 50, 120);
        AlternateScreenTableRenderer renderer = new AlternateScreenTableRenderer(terminal, config);

        renderer.enter(new String[]{"ID", "NAME"}, new Object[][]{{"1", "Al"}});

        termOutput.reset();
        // This page has much wider data that should trigger re-render.
        renderer.addPage(new Object[][]{{"2", "Alexander the Great"}});

        String output = termOutput.toString();
        // Full re-render should clear screen.
        assertThat(output, containsString(AlternateScreenTableRenderer.CLEAR_SCREEN));
        // Both old and new rows should be present.
        assertThat(output, containsString("Al"));
        assertThat(output, containsString("Alexander the Great"));

        renderer.leave();
    }

    @Test
    void leaveEmitsExitSequenceAndIsIdempotent() {
        TruncationConfig config = new TruncationConfig(true, 50, 120);
        AlternateScreenTableRenderer renderer = new AlternateScreenTableRenderer(terminal, config);

        renderer.enter(new String[]{"ID"}, new Object[][]{{"1"}});
        assertTrue(renderer.isActive());

        termOutput.reset();
        renderer.leave();

        String output = termOutput.toString();
        assertThat(output, containsString(AlternateScreenTableRenderer.RESET_SCROLL_REGION));
        assertThat(output, containsString(AlternateScreenTableRenderer.LEAVE_ALT_SCREEN));
        assertFalse(renderer.isActive());

        // Second leave should be a no-op.
        termOutput.reset();
        renderer.leave();
        assertThat(termOutput.toString(), is(""));
    }

    @Test
    void renderFinalTableProducesValidBoxDrawingOutput() {
        TruncationConfig config = new TruncationConfig(true, 50, 120);
        AlternateScreenTableRenderer renderer = new AlternateScreenTableRenderer(terminal, config);

        renderer.enter(new String[]{"ID", "NAME"}, new Object[][]{{"1", "Alice"}, {"2", "Bob"}});

        String finalTable = renderer.renderFinalTable();

        // Should contain box-drawing characters.
        assertThat(finalTable, containsString("\u2554")); // ╔
        assertThat(finalTable, containsString("\u255A")); // ╚
        // Should contain data.
        assertThat(finalTable, containsString("ID"));
        assertThat(finalTable, containsString("NAME"));
        assertThat(finalTable, containsString("Alice"));
        assertThat(finalTable, containsString("Bob"));
        // Should end with footer border.
        assertThat(finalTable.trim().charAt(finalTable.trim().length() - 1), is('\u255D')); // ╝

        renderer.leave();
    }

    @Test
    void finishShowsStatusAndWaitsForKeypress() throws Exception {
        TruncationConfig config = new TruncationConfig(true, 50, 120);
        AlternateScreenTableRenderer renderer = new AlternateScreenTableRenderer(terminal, config);

        renderer.enter(new String[]{"ID"}, new Object[][]{{"1"}});

        termOutput.reset();
        // Call leave() from a background thread to unblock finish()'s read loop.
        Thread unblockThread = new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
                // Ignore.
            }
            renderer.leave();
        });
        unblockThread.start();
        renderer.finish(1, 42, true);
        unblockThread.join(2000);

        String output = termOutput.toString();
        assertThat(output, containsString("1 row(s)"));
        assertThat(output, containsString("42 ms"));
        assertThat(output, containsString("Press any key to return..."));
    }

    @Test
    void finishWithoutTimingOmitsDuration() throws Exception {
        TruncationConfig config = new TruncationConfig(true, 50, 120);
        AlternateScreenTableRenderer renderer = new AlternateScreenTableRenderer(terminal, config);

        renderer.enter(new String[]{"ID"}, new Object[][]{{"1"}});

        termOutput.reset();
        // Call leave() from a background thread to unblock finish()'s read loop.
        Thread unblockThread = new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
                // Ignore.
            }
            renderer.leave();
        });
        unblockThread.start();
        renderer.finish(5, 100, false);
        unblockThread.join(2000);

        String output = termOutput.toString();
        assertThat(output, containsString("5 row(s)"));
        assertThat(output, not(containsString("100 ms")));
    }

    @Test
    void addPageOnInactiveRendererIsNoOp() {
        TruncationConfig config = new TruncationConfig(true, 50, 120);
        AlternateScreenTableRenderer renderer = new AlternateScreenTableRenderer(terminal, config);

        renderer.enter(new String[]{"ID"}, new Object[][]{{"1"}});
        renderer.leave();

        termOutput.reset();
        renderer.addPage(new Object[][]{{"2"}});

        // Nothing should be written.
        assertThat(termOutput.toString(), is(""));
    }
}
