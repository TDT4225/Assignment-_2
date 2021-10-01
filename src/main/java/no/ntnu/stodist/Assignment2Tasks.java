package no.ntnu.stodist;

import lombok.Data;
import no.ntnu.stodist.models.Activity;
import no.ntnu.stodist.models.TrackPoint;
import no.ntnu.stodist.models.User;
import no.ntnu.stodist.simpleTable.Column;
import no.ntnu.stodist.simpleTable.SimpleTable;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;


public class Assignment2Tasks {


    public static double haversine_distance(double lat1, double lat2, double lon1, double lon2) {

        lon1 = Math.toRadians(lon1);
        lon2 = Math.toRadians(lon2);
        lat1 = Math.toRadians(lat1);
        lat2 = Math.toRadians(lat2);

        double dlon = lon2 - lon1;
        double dlat = lat2 - lat1;
        double a = Math.pow(Math.sin(dlat / 2), 2)
                + Math.cos(lat1) * Math.cos(lat2)
                * Math.pow(Math.sin(dlon / 2), 2);
        double c = 2 * Math.asin(Math.sqrt(a));

        double r = 6371;
        return c * r;
    }

    private static LocalDateTime[] getTimeOverlap(LocalDateTime t1_start,
                                                  LocalDateTime t1_end,
                                                  LocalDateTime t2_start,
                                                  LocalDateTime t2_end
    ) {
        LocalDateTime start_overlap = t1_start.compareTo(t2_start) <= 0 ? t2_start : t1_start;
        LocalDateTime end_overlap   = t1_end.compareTo(t2_end) <= 0 ? t1_end : t2_end;
        return new LocalDateTime[]{start_overlap, end_overlap};
    }

    private static boolean isTimeOverlap(LocalDateTime t1_start,
                                         LocalDateTime t1_end,
                                         LocalDateTime t2_start,
                                         LocalDateTime t2_end
    ) {
        return t1_start.isBefore(t2_end) && t2_start.isBefore(t1_end);
    }

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

        StringBuilder userQueryStringBuilder = new StringBuilder(userQueryBase);
        StringBuilder actQueryStringBuilder  = new StringBuilder(actQueryBase);

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
                String query = userPointQueryStringB.toString();
                if (query.length() > trackQueryBase.length() + 10) {
                    var statement = connection.createStatement();
                    statement.setQueryTimeout(100);
                    statement.execute(query);
                    System.out.println("Sucsessfully added data for " + user.getId());
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }


        });
    }

    private static SimpleTable makeResultSetTable(ResultSet resultSet) throws SQLException {
        SimpleTable<List<String>> simpleTable = new SimpleTable<>();

        ResultSetMetaData metaData = resultSet.getMetaData();

        List<List<String>> tabelData = new ArrayList<>();
        List<String>       headerRow = new ArrayList<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            headerRow.add(metaData.getColumnLabel(i));
        }

        while (resultSet.next()) {
            List<String> rowData = new ArrayList<>();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                rowData.add(resultSet.getString(i));
            }
            tabelData.add(rowData);

        }

        List<Column<List<String>>> tabelCols = new ArrayList<>();
        for (int i = 0; i < headerRow.size(); i++) {
            int finalI = i;
            tabelCols.add(new Column<>(headerRow.get(i), row -> row.get(finalI)));
        }

        simpleTable.getCols().addAll(tabelCols);
        simpleTable.setItems(tabelData);
        return simpleTable;


    }

    public static void task1(Connection connection) throws SQLException {

        String query = """
                       SELECT
                          (SELECT COUNT(*) FROM user) as users,
                          (SELECT COUNT(*) FROM activity) as activities,
                          (SELECT COUNT(*) FROM track_point) as tps
                       """;

        ResultSet   resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 1");
        simpleTable.display();

    }

    public static void task2(Connection connection) throws SQLException {
        String query = """
                       SELECT AVG(a.count) AS avg
                       FROM ( SELECT count(*) AS count, user_id
                              FROM activity
                              GROUP BY user_id) AS a
                        """;
        ResultSet   resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 2");
        simpleTable.display();


    }

    public static void task3(Connection connection) throws SQLException {
        String query = """
                       SELECT user_id, COUNT(user_id) as num_activities
                       from activity
                       group by user_id
                       order by num_activities DESC 
                       limit 10
                       """;
        ResultSet   resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 3");
        simpleTable.display();
    }

    public static void task4(Connection connection) throws SQLException {


        String query = """
                       SELECT COUNT(DISTINCT c.user_id)
                       FROM(SELECT user_id
                            FROM activity
                            WHERE EXTRACT(DAY from start_date_time) != EXTRACT(DAY from end_date_time)) as c
                                                    
                        """;
        ResultSet   resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 4");
        simpleTable.display();

    }

    public static void task5(Connection connection) throws SQLException {
        String query = """
                       select a.id as duplicate_asignment_ids
                       from activity as a,
                           (
                               select user_id, start_date_time, end_date_time
                               from activity
                               group by user_id, start_date_time, end_date_time
                               having count(*) > 1
                           ) as f
                       where a.user_id = f.user_id
                       AND  a.start_date_time = f.start_date_time
                       AND a.end_date_time = f.end_date_time;
                       """;
        ResultSet   resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 4");
        simpleTable.display();
    }

    //øystein
    public static void task6(Connection connection) throws SQLException {
        final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        String query = """
                                             
                       """;
        ResultSet t = connection.createStatement().executeQuery(query);

        while (t.next()) {
            break;
        }


    }

    public static void task7(Connection connection) throws SQLException {


        String query = """
                       SELECT id
                       FROM user
                       WHERE id NOT IN (
                                       SELECT DISTINCT c.user_id
                                       FROM(
                                               SELECT user_id, transportation_mode
                                               FROM activity
                                               WHERE transportation_mode = "taxi") as c);
                       """;
        ResultSet   resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 7");
        simpleTable.display();

    }


    public static void task8(Connection connection) throws SQLException {

        System.out.println("Task8");

        String query = """
                       SELECT transportation_mode, COUNT(DISTINCT user_id)
                       FROM activity
                       WHERE transportation_mode IS NOT NULL
                       GROUP BY transportation_mode;        
                       """;
        ResultSet t = connection.createStatement().executeQuery(query);

    }

    public static void task9(Connection connection) throws SQLException {
        String query = """
                       SELECT
                       """;
        ResultSet   resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 3");
        simpleTable.display();
    }

    public static void task10(Connection connection) throws SQLException {
        String query = """
                       SELECT
                       """;
        ResultSet   resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 3");
        simpleTable.display();
    }

    //øystein
    public static void task11(Connection connection) throws SQLException {
        String query = """
                       SELECT
                       """;
        ResultSet   resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 3");
        simpleTable.display();
    }

    public static void task12(Connection connection) throws SQLException {
        String query = """
                       SELECT
                       """;
        ResultSet   resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 3");
        simpleTable.display();
    }
}
