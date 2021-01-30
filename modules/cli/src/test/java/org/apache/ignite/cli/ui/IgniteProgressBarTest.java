package org.apache.ignite.cli.ui;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class IgniteProgressBarTest {
    @Test
    public void test() {
        var outputStream = new ByteArrayOutputStream();
        var out = new PrintWriter(outputStream, true);
        var progressBar = new IgniteProgressBar(out, 3, 80);
        progressBar.step();
        assertEquals(
            "\r|=======================>                                                 | 33%",
            outputStream.toString()
        );
        progressBar.step();
        assertEquals(
            "\r|=======================>                                                 | 33%" +
            "\r|===============================================>                         | 66%",
            outputStream.toString()
        );
        progressBar.step();
        assertEquals(
            "\r|=======================>                                                 | 33%" +
                "\r|===============================================>                         | 66%" +
            "\r|=========================================================================|Done!",
            outputStream.toString()
        );
        progressBar.step();
        assertEquals(
            "\r|=======================>                                                 | 33%" +
                "\r|===============================================>                         | 66%" +
                "\r|=========================================================================|Done!" +
            "\r|=========================================================================|Done!",
            outputStream.toString()
        );
    }
}
