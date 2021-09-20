package no.ntnu.stodist;

import no.ntnu.stodist.database.MySqlConnection;

import java.sql.Connection;
import java.sql.SQLException;

public class main {

    public static void main(String[] args) {
        System.out.println("hello");
        MySqlConnection db_connection = new MySqlConnection();

        try {
            Connection connection = db_connection.getConnection();

            Assignment2Tasks.task1(connection);
            Assignment2Tasks.task2(connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
