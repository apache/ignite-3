package org.apache.ignite.internal.cli.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PlainTableRendererTest {

    @Test
    void testRender() {
        String[] header = new String[]{"id", "name", "address"};
        Object[] row1 = new Object[]{1, "John", null};
        Object[] row2 = new Object[]{2, "Jessica", "any address"};
        Object[][] content = new Object[][]{row1, row2};
        String render = PlainTableRenderer.render(header, content);
        String[] renderedRows = render.split("\n");
        Assertions.assertEquals(3, renderedRows.length);
        for (String renderedRow : renderedRows) {
            Assertions.assertEquals(3, renderedRow.split("\t").length);
        }
    }
}
