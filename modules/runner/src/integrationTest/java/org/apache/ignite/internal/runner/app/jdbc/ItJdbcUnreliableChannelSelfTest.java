package org.apache.ignite.internal.runner.app.jdbc;

import java.lang.reflect.Field;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.ignite.internal.client.TcpIgniteClient;
import org.apache.ignite.internal.jdbc.JdbcConnection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ItJdbcUnreliableChannelSelfTest extends ItJdbcAbstractStatementSelfTest {

    /**
     * Test case: select * from, stop node,
     * */
    @Test
    public void firstTest() throws Exception {
        Statement statement = conn.createStatement();

        ResultSet resultSet = statement.executeQuery("select * from PERSON");
        assert !resultSet.isClosed();

        Class<?> connectionClass = JdbcConnection.class;

        Field field = connectionClass.getDeclaredField("client");
        field.setAccessible(true);

        TcpIgniteClient client = (TcpIgniteClient) field.get(conn);

        client.close();

        conn = DriverManager.getConnection(URL);
    }

    @Test
    public void secondTest() throws Exception {
        Statement statement = conn.createStatement();

        ResultSet resultSet = statement.executeQuery("select * from PERSON");
        assert !resultSet.isClosed();

        Class<?> connectionClass = JdbcConnection.class;

        Field field = connectionClass.getDeclaredField("client");
        field.setAccessible(true);

        TcpIgniteClient client = (TcpIgniteClient) field.get(conn);

        client.close();

        Thread.sleep(2000);

        assert !resultSet.isClosed();

        statement.executeQuery("select * from PERSON");

        conn = DriverManager.getConnection(URL);


    }
}
