package no.ntnu.stodist;

import no.ntnu.stodist.database.MySqlConnection;
import no.ntnu.stodist.models.User;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class Assignment2Tasks {

    public static void crateTables(Connection connection) throws SQLException {
        String createUser = """
                            CREATE TABLE if not exists user (
                                id int primary key auto_increment,
                                has_labels boolean
                            ); 
                            """;

        String createAct = """
                           CREATE TABLE if not exists activity (
                                id int primary key auto_increment,
                                user_id int references user(id),
                                transportation_mode text,
                                start_date_time datetime,
                                end_date_time datetime
                            ); 
                                                      """;


        String createTrack = """
                             CREATE TABLE if not exists track_point(
                                 id int primary key auto_increment,
                                 activity_id int references activity(id),
                                 lat double,
                                 lon double,
                                 altitude int,
                                 data_days double,
                                 date_time datetime
                             );
                             """;
        connection.createStatement().execute(createUser);

        connection.createStatement().execute(createAct);

        connection.createStatement().execute(createTrack);

    }

    public static void insertData(Connection connection) throws SQLException, IOException {

        String userQueryBase  = "INSERT INTO user (id, has_labels)  VALUES";
        String actQueryBase   = "INSERT INTO activity (id, user_id, transportation_mode, start_date_time, end_date_time)  VALUES ";
        String trackQueryBase = "INSERT INTO track_point (id, activity_id, lat, lon, altitude, data_days, date_time)  VALUES ";


        DatasetParser datasetParser = new DatasetParser();
        File          datasetDir    = new File("/datasett/Data/");

        List<User> users = datasetParser.parseDataset(datasetDir);

        StringBuilder userQueryStringBuilder  = new StringBuilder(userQueryBase);
        StringBuilder actQueryStringBuilder   = new StringBuilder(actQueryBase);
        StringBuilder trackQueryStringBuilder = new StringBuilder(trackQueryBase);

        // insert the user values
        users.forEach(user -> userQueryStringBuilder.append("('")
                                                    .append(user.getId())
                                                    .append("', ")
                                                    .append((user.isHasLabels()) ? "TRUE" : "FALSE")
                                                    .append(" ), "));
        userQueryStringBuilder.deleteCharAt(userQueryStringBuilder.length() - 1);
        userQueryStringBuilder.deleteCharAt(userQueryStringBuilder.length() - 1);
        userQueryStringBuilder.append(";");

        connection.createStatement().execute(userQueryStringBuilder.toString());

        // insert the act values
        users.forEach(user -> user.getActivities().forEach(activity -> actQueryStringBuilder.append("('")
                                                                                            .append(activity.getId())
                                                                                            .append("', '")
                                                                                            .append(user.getId())
                                                                                            .append("', ")
                                                                                            .append(activity.getTransportationMode()
                                                                                                            .map(s -> "'" + s + "'")
                                                                                                            .orElse("NULL"))
                                                                                            .append(", '")
                                                                                            .append(activity.getStartDateTime())
                                                                                            .append("', '")
                                                                                            .append(activity.getEndDateTime())
                                                                                            .append("'), ")));
        actQueryStringBuilder.deleteCharAt(actQueryStringBuilder.length() - 1);
        actQueryStringBuilder.deleteCharAt(actQueryStringBuilder.length() - 1);
        actQueryStringBuilder.append(";");

        connection.createStatement().execute(actQueryStringBuilder.toString());

        // insert the point values


        users.forEach(user -> {
            // have to split it up if not the query string ends up a 1 010 534 900 chars long and something rolls over
            StringBuilder userPointQueryStringB = new StringBuilder(trackQueryBase);
            user.getActivities()
                .forEach(activity -> activity.getTrackPoints()
                                             .forEach(trackPoint -> userPointQueryStringB.append(
                                                                                                 "('")
                                                                                         .append(trackPoint.getId())
                                                                                         .append("', '")
                                                                                         .append(activity.getId())
                                                                                         .append("', '")
                                                                                         .append(trackPoint.getLatitude())
                                                                                         .append("', '")
                                                                                         .append(trackPoint.getLongitude())
                                                                                         .append("', ")
                                                                                         .append(trackPoint.getAltitude()
                                                                                                           .map(s -> "'" + s + "'")
                                                                                                           .orElse("NULL"))
                                                                                         .append(", '")
                                                                                         .append(trackPoint.getDateDays())
                                                                                         .append("', '")
                                                                                         .append(trackPoint.getDateTime())
                                                                                         .append("'), ")));
            userPointQueryStringB.deleteCharAt(userPointQueryStringB.length() - 1);
            userPointQueryStringB.deleteCharAt(userPointQueryStringB.length() - 1);
            userPointQueryStringB.append(";");


            try {
                String query = userQueryBase.toString();
                if (query.length() > trackQueryBase.length() + 10) {
                    connection.createStatement().execute(userPointQueryStringB.toString());
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }


        });
    }


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
