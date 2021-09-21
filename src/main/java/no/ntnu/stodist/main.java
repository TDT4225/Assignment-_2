package no.ntnu.stodist;

import no.ntnu.stodist.database.MySqlConnection;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public class main {

    public static void main(String[] args) {
        MySqlConnection db_connection = new MySqlConnection();
        try {

            Connection connection = db_connection.getConnection();
            SimpleStopwatch.start("a");
            Assignment2Tasks.crateTables(connection);
            Assignment2Tasks.insertData(connection);

            SimpleStopwatch.stop("a");
            SimpleStopwatch.show("a");
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }
}
