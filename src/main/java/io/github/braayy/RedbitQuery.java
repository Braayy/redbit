package io.github.braayy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class RedbitQuery implements AutoCloseable {

    private final Connection connection;
    private final PreparedStatement statement;

    public RedbitQuery(Connection connection, PreparedStatement statement) {
        this.connection = connection;
        this.statement = statement;
    }

    public ResultSet executeQuery() throws SQLException {
        return this.statement.executeQuery();
    }

    public void executeUpdate() throws SQLException {
        this.statement.executeUpdate();
    }

    public Connection getConnection() {
        return connection;
    }

    public PreparedStatement getStatement() {
        return statement;
    }

    @Override
    public void close() throws SQLException {
        if (connection != null) connection.close();
        if (statement != null) statement.close();
    }
}
