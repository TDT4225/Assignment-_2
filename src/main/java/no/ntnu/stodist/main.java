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
            //            Assignment2Tasks.crateTables(connection);
            //            Assignment2Tasks.insertData(connection);
            Assignment2Tasks.task1(connection);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
