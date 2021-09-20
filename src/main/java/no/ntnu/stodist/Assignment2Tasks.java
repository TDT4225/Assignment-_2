package no.ntnu.stodist;

import no.ntnu.stodist.database.MySqlConnection;

import java.sql.Connection;
import java.sql.SQLException;

public class Assignment2Tasks {
    

    public static void task1(Connection connection) throws SQLException {

        String query = """
                       SELECT *
                       FROM stodist; 
                       """;

        connection.createStatement().execute(query);

    }

    public static void task2(Connection connection) throws SQLException {

    }
}
