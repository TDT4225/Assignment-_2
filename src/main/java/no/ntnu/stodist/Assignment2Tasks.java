package no.ntnu.stodist;

import lombok.Data;
import no.ntnu.stodist.models.Activity;
import no.ntnu.stodist.models.TrackPoint;
import no.ntnu.stodist.models.User;
import no.ntnu.stodist.simpleTable.Column;
import no.ntnu.stodist.simpleTable.SimpleTable;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.chrono.ChronoPeriod;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class Assignment2Tasks {

    /**
     * Calculates the haversine distance between two coordinates in km
     *
     * @param lat1 lat of the first cord
     * @param lat2 lat of the second cord
     * @param lon1 lon of the first cord
     * @param lon2 lon of the second cord
     *
     * @return Returns the distance in Km between the points
     */
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


    /**
     * Get the overlap between two LocalDateTime ranges
     *
     * @param t1_start start of first datetime
     * @param t1_end   end of first datetime
     * @param t2_start start of second datetim
     * @param t2_end   end of second datetime
     *
     * @return Array with start and end LocalDateTime
     */
    private static LocalDateTime[] getTimeOverlap(LocalDateTime t1_start,
                                                  LocalDateTime t1_end,
                                                  LocalDateTime t2_start,
                                                  LocalDateTime t2_end
    ) {
        LocalDateTime start_overlap = t1_start.compareTo(t2_start) <= 0 ? t2_start : t1_start;
        LocalDateTime end_overlap   = t1_end.compareTo(t2_end) <= 0 ? t1_end : t2_end;
        return new LocalDateTime[]{start_overlap, end_overlap};
    }

    /**
     * Return whether two LocalDatetime ranges overlap
     *
     * @param t1_start start of first datetime
     * @param t1_end   end of first datetime
     * @param t2_start start of second datetim
     * @param t2_end   end of second datetime
     *
     * @return boolean, true if overlap, false if not
     */
    private static boolean isTimeOverlap(LocalDateTime t1_start,
                                         LocalDateTime t1_end,
                                         LocalDateTime t2_start,
                                         LocalDateTime t2_end
    ) {
        return t1_start.isBefore(t2_end) && t2_start.isBefore(t1_end);
    }

    public static void crateTables(Connection connection) throws SQLException {
        String createUser = """
                            CREATE TABLE IF NOT EXISTS user (
                                id INT PRIMARY KEY AUTO_INCREMENT,
                                has_labels BOOLEAN
                            ); 
                            """;

        String createAct = """
                           CREATE TABLE IF NOT EXISTS activity (
                                id INT PRIMARY KEY AUTO_INCREMENT,
                                user_id INT REFERENCES user(id) ON DELETE CASCADE ON UPDATE CASCADE,
                                transportation_mode TEXT,
                                start_date_time DATETIME,
                                end_date_time DATETIME
                            ); 
                                                      """;


        String createTrack = """
                             CREATE TABLE IF NOT EXISTS track_point(
                                 id INT PRIMARY KEY AUTO_INCREMENT,
                                 activity_id INT REFERENCES activity(id) ON DELETE CASCADE ON UPDATE CASCADE,
                                 lat DOUBLE,
                                 lon DOUBLE,
                                 altitude INT,
                                 data_days DOUBLE,
                                 date_time DATETIME
                             );
                             """;
        connection.createStatement().execute(createUser);

        connection.createStatement().execute(createAct);

        connection.createStatement().execute(createTrack);

    }

    public static void insertData(Connection connection) throws SQLException, IOException {

        String userQueryBase  = "INSERT INTO user (id, has_labels)  VALUES ";
        String actQueryBase   = "INSERT INTO activity (id, user_id, transportation_mode, start_date_time, end_date_time)  VALUES ";
        String trackQueryBase = "INSERT INTO track_point (id, activity_id, lat, lon, altitude, data_days, date_time)  VALUES ";


        DatasetParser datasetParser = new DatasetParser();
        File          datasetDir    = new File("/datasett/Data/");

        List<User> users = datasetParser.parseDataset(datasetDir);

        //        users.stream()
        //             .filter(user -> user.getId() == 113)
        //             .forEach(user -> user.getActivities()
        //                                  .forEach(activity -> System.out.println(activity.getTransportationMode())));
        //        System.exit(0);

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

    private static SimpleTable<List<String>> makeResultSetTable(ResultSet resultSet) throws SQLException {
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
        simpleTable.setTopPaddingLines(1);
        simpleTable.setItems(tabelData);
        return simpleTable;


    }

    public static void task1(Connection connection) throws SQLException {
        String query = """
                       SELECT
                          (SELECT COUNT(*) FROM user) AS users,
                          (SELECT COUNT(*) FROM activity) AS activities,
                          (SELECT COUNT(*) FROM track_point) AS tps
                       """;

        ResultSet                 resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable<List<String>> simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 1");
        simpleTable.display();
    }

    public static void task2(Connection connection) throws SQLException {
        String query = """
                       SELECT AVG(t.activity_count) AS avg, MIN(t.activity_count) AS min, MAX(t.activity_count) AS max
                       FROM (
                            SELECT u.id, COUNT(a.user_id) AS activity_count
                            FROM user u
                            LEFT JOIN activity AS a ON u.id = a.user_id
                            GROUP BY u.id
                            ) AS t    
                       """;
        ResultSet   resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 2");
        simpleTable.display();


    }

    public static void task3(Connection connection) throws SQLException {
        String query = """
                       SELECT user_id, COUNT(user_id) AS num_activities
                       FROM activity
                       GROUP BY user_id
                       ORDER BY num_activities DESC 
                       LIMIT 10
                       """;
        ResultSet   resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 3");
        simpleTable.display();
    }

    public static void task4(Connection connection) throws SQLException {
        String query = """
                       SELECT COUNT(DISTINCT c.user_id) AS num_users
                       FROM(SELECT user_id
                            FROM activity
                            WHERE DATEDIFF(end_date_time, start_date_time) = 1) AS c
                                                    
                       """;
        ResultSet   resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 4");
        simpleTable.display();

    }

    public static void task5(Connection connection) throws SQLException {
        String query = """
                       SELECT a.id AS duplicate_asignment_ids
                       FROM activity AS a,
                           (
                               SELECT user_id, start_date_time, end_date_time
                               FROM activity
                               GROUP BY user_id, start_date_time, end_date_time
                               HAVING COUNT(*) > 1
                           ) AS f
                       WHERE a.user_id = f.user_id
                       AND  a.start_date_time = f.start_date_time
                       AND a.end_date_time = f.end_date_time;
                       """;
        ResultSet resultSet = connection.createStatement().executeQuery(query);
        resultSet.next();

        if (! resultSet.next()) {
            System.out.println("Task 5");
            System.out.println("no results");
        } else {
            SimpleTable simpleTable = makeResultSetTable(resultSet);
            simpleTable.setTitle("Task 5");
            simpleTable.display();
        }
    }

    public static void task6(Connection connection) throws SQLException {
        String query = """
                       WITH tps AS (
                           SELECT user_id, activity_id,tp.id AS act_id, lat, lon, date_time
                           FROM activity a
                                    JOIN track_point tp
                                         ON a.id = tp.activity_id)
                       SELECT DISTINCT tps.user_id AS user_1, tps2.user_id AS user_2
                       FROM tps
                       INNER JOIN tps AS tps2
                       ON tps.user_id != tps2.user_id
                       AND SECOND(ABS(TIMEDIFF(tps.date_time, tps2.date_time))) < 60
                       AND ST_DISTANCE(POINT(tps.lat, tps.lon), POINT(tps2.lat, tps2.lon)) < 100;                    
                       """;
        ResultSet   resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 6");
        simpleTable.display();
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
                                               WHERE transportation_mode = 'taxi') AS c);
                       """;
        ResultSet   resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 7");
        simpleTable.display();

    }


    public static void task8(Connection connection) throws SQLException {
        String query = """
                       SELECT transportation_mode, COUNT(DISTINCT user_id)
                       FROM activity
                       WHERE transportation_mode IS NOT NULL
                       GROUP BY transportation_mode;        
                       """;
        ResultSet                 resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable<List<String>> simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 8");
        simpleTable.display();
    }

    private static void task9a(Connection connection) throws SQLException {
        String query = """
                       SELECT COUNT(*) AS num_activites, YEAR(activity.start_date_time) AS year, MONTH(activity.start_date_time) AS month
                       FROM activity
                       GROUP BY YEAR(activity.start_date_time), MONTH(activity.start_date_time)
                       ORDER BY COUNT(*) DESC
                       LIMIT 1;
                       """;
        ResultSet                 resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable<List<String>> simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 9a");
        simpleTable.display();
    }

    private static void task9b(Connection connection) throws SQLException {
        String query = """
                       SELECT COUNT(a.user_id) AS num_activities, a.user_id, SUM(TIMEDIFF(a.end_date_time,a.start_date_time)) AS time_spent
                       FROM
                            activity AS a,
                            (SELECT COUNT(*) AS num_activites, YEAR(activity.start_date_time) AS year, MONTH(activity.start_date_time) AS month
                             FROM activity
                             GROUP BY YEAR(activity.start_date_time), MONTH(activity.start_date_time)
                             ORDER BY COUNT(*) DESC
                             LIMIT 1) AS best_t
                       WHERE YEAR(a.start_date_time) = best_t.year
                       AND MONTH(a.start_date_time) = best_t.month
                       GROUP BY user_id
                       ORDER BY COUNT(a.user_id) DESC
                       LIMIT 2;
                       """;
        ResultSet                 resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable<List<String>> simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 9b");
        simpleTable.display();

        List<List<String>> tabelData = simpleTable.getItems();

        var timeSpentTop    = Duration.ofSeconds(Long.parseLong(tabelData.get(0).get(2)));
        var timeSpentSecond = Duration.ofSeconds(Long.parseLong(tabelData.get(1).get(2)));

        int dif = timeSpentTop.compareTo(timeSpentSecond);
        if (dif > 0) {
            System.out.printf(
                    "the user with the most activities, user: %s spent more time activitying than the user with the second most activities\n",
                    tabelData.get(0).get(0)
            );
        } else if (dif < 0) {
            System.out.printf(
                    "the user with the next most activities, user: %s spent more time activitying than the user with the second most activities\n",
                    tabelData.get(1).get(0)
            );
        } else {
            System.out.println("the top and next top users spent the same time activitying");
        }

        System.out.printf("user: %-4s with most activities spent     : Hours: %-3s Min: %-2s Sec: %s\n",
                          tabelData.get(0).get(0),
                          timeSpentTop.toHours(),
                          timeSpentTop.minusHours(timeSpentTop.toHours()).toMinutes(),
                          timeSpentTop.minusMinutes(timeSpentTop.toMinutes())
                                      .toSeconds()
        );
        System.out.printf("user: %-4s with next most activities spent: Hours: %-3s Min: %-2s Sec: %s\n",
                          tabelData.get(1).get(0),
                          timeSpentSecond.toHours(),
                          timeSpentSecond.minusHours(timeSpentSecond.toHours()).toMinutes(),
                          timeSpentSecond.minusMinutes(timeSpentSecond.toMinutes())
                                         .toSeconds()
        );


    }

    public static void task9(Connection connection) throws SQLException {
        task9a(connection);
        task9b(connection);
    }

    public static void task10(Connection connection) throws SQLException {
        String query = """
                       SELECT tp.*
                       FROM (SELECT * FROM user WHERE user.id = 113) AS u # we use 113 instead of 112 because the indexes are shifted
                       INNER JOIN (
                           SELECT *
                           FROM activity
                           WHERE YEAR(start_date_time) = 2008
                           AND transportation_mode = 'walk'
                           ) a
                       ON u.id = a.user_id
                       INNER JOIN track_point tp
                       ON a.id = tp.activity_id
                       """;
        ResultSet resultSet = connection.createStatement().executeQuery(query);

        HashMap<Integer, Activity> activities = new HashMap<>();

        while (resultSet.next()) {
            TrackPoint trackPoint = new TrackPoint();
            trackPoint.setLatitude(resultSet.getDouble(3));
            trackPoint.setLongitude(resultSet.getDouble(4));

            int activityId = resultSet.getInt(2);
            if (! activities.containsKey(activityId)) {
                activities.put(activityId, new Activity(activityId));
            }
            Activity parentAct = activities.get(activityId);
            parentAct.getTrackPoints().add(trackPoint);
        }

        double totDist = activities.values().stream().parallel().map(activity -> {
            TrackPoint keep = null;
            double     roll = 0;
            for (TrackPoint trackPoint : activity.getTrackPoints()) {
                if (keep == null) {
                    keep = trackPoint;
                    continue;
                }
                roll += haversine_distance(trackPoint.getLatitude(),
                                           keep.getLatitude(),
                                           trackPoint.getLongitude(),
                                           keep.getLongitude()
                );
                keep = trackPoint;

            }
            return roll;
        }).reduce(Double::sum).get();

        System.out.println("\tTask 10");
        System.out.printf("User 112 have in total walked %.3f km in 2008\n", totDist);

    }

    public static void task11(Connection connection) throws SQLException {
        String query = """
                       WITH delta_alt_tps AS (
                           SELECT track_point.id, track_point.activity_id , LEAD(track_point.altitude) OVER (PARTITION BY track_point.activity_id ORDER BY id) - track_point.altitude AS delta_alt
                           FROM track_point
                           WHERE track_point.altitude != -777
                           AND track_point.altitude IS NOT NULL
                       ),
                       delta_alt_act AS (
                           SELECT delta_alt_tps.activity_id, SUM(IF(delta_alt_tps.delta_alt > 0, delta_alt_tps.delta_alt, 0)) AS altitude_gain
                           FROM delta_alt_tps
                           GROUP BY delta_alt_tps.activity_id)
                       SELECT activity.user_id, (SUM(delta_alt_act.altitude_gain)/ 3.2808) AS user_altitude_gain_m
                       FROM activity
                                JOIN delta_alt_act ON activity.id = delta_alt_act.activity_id
                       GROUP BY activity.user_id
                       ORDER BY  user_altitude_gain_m DESC
                       LIMIT 20;  
                       """;
        ResultSet   resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 11");
        simpleTable.display();
    }

    public static void task12(Connection connection) throws SQLException {
        String query = """
                       WITH shifted_tps AS (
                           SELECT
                               track_point.activity_id,
                               track_point.date_time,
                               LEAD(track_point.date_time)
                                   OVER (PARTITION BY track_point.activity_id ORDER BY track_point.id) AS shifted_time
                           FROM
                               track_point
                       )
                       SELECT activity.user_id, COUNT(*) AS num_invalid_activities
                       FROM (
                               SELECT shifted_tps.activity_id, MINUTE(TIMEDIFF(shifted_tps.date_time, shifted_tps.shifted_time)) AS td
                               FROM shifted_tps
                               WHERE MINUTE(TIMEDIFF(shifted_tps.date_time, shifted_tps.shifted_time)) > 5
                       ) AS invalid_acts
                       INNER JOIN activity
                       ON invalid_acts.activity_id = activity.id
                       GROUP BY activity.user_id
                       ORDER BY num_invalid_activities DESC;
                       """;
        ResultSet   resultSet   = connection.createStatement().executeQuery(query);
        SimpleTable simpleTable = makeResultSetTable(resultSet);
        simpleTable.setTitle("Task 12");
        simpleTable.display();
    }
}
