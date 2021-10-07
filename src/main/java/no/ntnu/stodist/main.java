package no.ntnu.stodist;

import no.ntnu.stodist.database.MySqlConnection;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public class main {

    public static void main(String[] args) {
        MySqlConnection db_connection = new MySqlConnection();
        System.out.println();
        try {

            Connection connection = db_connection.getConnection();
            Assignment2Tasks.crateTables(connection);
            //Assignment2Tasks.insertData(connection);
            Assignment2Tasks.task1(connection);
            Assignment2Tasks.task2(connection);
            Assignment2Tasks.task3(connection);
            Assignment2Tasks.task4(connection);
            Assignment2Tasks.task5(connection);
            //Assignment2Tasks.task6(connection);
            Assignment2Tasks.task7(connection);
            Assignment2Tasks.task8(connection);
            Assignment2Tasks.task9(connection);
            Assignment2Tasks.task10(connection);
            Assignment2Tasks.task11(connection);
            Assignment2Tasks.task12(connection);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
