package no.ntnu.stodist;

import no.ntnu.stodist.debugLogger.DebugLogger;
import no.ntnu.stodist.models.Activity;
import no.ntnu.stodist.models.TrackPoint;
import no.ntnu.stodist.models.User;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class DatasetParser {

    private static DebugLogger dbl = new DebugLogger(true);
    private static final String labelFileName = "labels.txt";
    private static final String dataDirName = "Trajectory";

    private DateTimeFormatter labelDateFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
    private DateTimeFormatter pointDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-ddHH:mm:ss");

    public List<User> parseDataset(File rootDir) throws IOException {

        ArrayList<User> retList   = new ArrayList<>();
        var             usersDirs = Files.list(rootDir.toPath()).sorted().toList();

        for (Path path : usersDirs) {
            retList.add(this.parseUserDir(path.toFile()));
        }


        return retList;


    }

    public User parseUserDir(File userDir) {
        try {
            User user = new User();
            user.setHasLabels(false);

            File labelsFile = new File(userDir, labelFileName);

            List<File> fileList = Files.list(new File(userDir, dataDirName).toPath()).map(Path::toFile).toList();

            List<Activity> activities = fileList.stream().parallel().filter(file -> {
                try {
                    return Files.lines(file.toPath()).count() < 2506;
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException();
                }
            }).map(this::parsePltFile).toList();


            if (labelsFile.exists()) {
                user.setHasLabels(true);
                BufferedReader reader = new BufferedReader(new FileReader(labelsFile));
                reader.readLine();// skip first line with indexes.
                reader.lines().forEach(s -> {
                    String[]      parts        = s.split("\t");
                    LocalDateTime startTime    = LocalDateTime.parse(parts[0], labelDateFormatter);
                    LocalDateTime endTime      = LocalDateTime.parse(parts[1], labelDateFormatter);
                    String        activityType = parts[2];
                    activities.stream()
                              .filter(activity -> activity.getStartDateTime()
                                                          .isEqual(startTime) && activity.getEndDateTime()
                                                                                         .isEqual(endTime))
                              .findAny()
                              .ifPresent(activity -> activity.setTransportationMode(activityType));
                });


            }

            user.setActivities(activities);
            return user;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    private Activity parsePltFile(File pltFile) {

        try {
            BufferedReader reader = new BufferedReader(new FileReader(pltFile));

            for (int i = 0; i < 6; i++) {
                reader.readLine();
            }

            List<TrackPoint> points = reader.lines().map(line -> {
                String[] parts      = line.split(",");
                double   latitude   = Double.parseDouble(parts[0]);
                double   longditude = Double.parseDouble(parts[1]);
                Integer  altitude   = null;

                double        dateDays = Double.parseDouble(parts[4]);
                LocalDateTime dateTime = LocalDateTime.parse(parts[5] + parts[6], pointDateFormatter);

                try {
                    altitude = Integer.parseInt(parts[3]);
                } catch (NumberFormatException e) {

                }
                if (altitude != null && altitude == - 777) {
                    altitude = null;
                }

                TrackPoint trackPoint = new TrackPoint();
                trackPoint.setLatitude(latitude);
                trackPoint.setLongitude(longditude);
                trackPoint.setAltitude(altitude);
                trackPoint.setDateDays(dateDays);
                trackPoint.setDateTime(dateTime);

                return trackPoint;

            }).toList();

            Activity activity = new Activity();
            activity.setTrackPoints(points);
            activity.setStartDateTime(points.get(0).getDateTime());
            activity.setEndDateTime(points.get(points.size() - 1).getDateTime());

            return activity;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }


}
