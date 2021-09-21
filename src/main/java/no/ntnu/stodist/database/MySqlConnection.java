package no.ntnu.stodist.database;

import no.ntnu.stodist.debugLogger.DebugLogger;

import java.sql.*;


public class MySqlConnection {

    private Connection connection;
    private static final String url = System.getenv("SQL_URL"); //"jdbc:postgresql://10.10.50.50/fractal";
    private static final String dbUser = System.getenv("USER_USERNAME");
    private static final String dbPassword = System.getenv("USER_PASSWORD");
    //    private final String url;
    //    private final String dbUser;
    //    private final String dbPassword;

    protected static final DebugLogger dbl = new DebugLogger(false);


    /**
     * Try to connect to the db
     *
     * @return a connection object, null if unsuccessful.
     */
    private Connection tryConnectToDB() throws SQLException {
        dbl.log("try connect to db", "url", url, "user", dbUser, "passwd", dbPassword);
        Connection connection = null;

        connection = DriverManager.getConnection(url, dbUser, dbPassword);
        
        return connection;
    }

    public Connection getConnection() throws SQLException {
        if (connection == null) {
            connection = this.tryConnectToDB();
        } else if (connection.isClosed()) {
            connection = this.tryConnectToDB();
        }
        return connection;


    }


}
