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
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class DatasetParser {

    private static final DebugLogger dbl = new DebugLogger(true);
    private static final String labelFileName = "labels.txt";
    private static final String dataDirName = "Trajectory";

    private final DateTimeFormatter labelDateFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
    private final DateTimeFormatter pointDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-ddHH:mm:ss");

    public static void main(String[] args) {
        try {
            var a = new File("/home/trygve/development/skole/stodis/Assignment_2/datasett/Data");
            var b = new DatasetParser().parseDataset(a);


            b.stream()
             .filter(user -> user.getId() == 111)
             .forEach(user -> user.getActivities()
                                  .forEach(activity -> System.out.println(activity.getTransportationMode())));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

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

            File           labelsFile = new File(userDir, labelFileName);
            List<Activity> activities = new ArrayList<>();

            if (labelsFile.exists()) {
                user.setHasLabels(true);
                BufferedReader reader = new BufferedReader(new FileReader(labelsFile));
                reader.readLine();// skip first line with indexes.

                reader.lines().forEach(s -> {
                    String[]      parts        = s.split("\t");
                    LocalDateTime startTime    = LocalDateTime.parse(parts[0], labelDateFormatter);
                    LocalDateTime endTime      = LocalDateTime.parse(parts[1], labelDateFormatter);
                    String        activityType = parts[2];

                    Activity activity = new Activity();

                    activity.setStartDateTime(startTime);
                    activity.setEndDateTime(endTime);
                    activity.setTransportationMode(activityType);
                    activities.add(activity);
                });

                activities.sort(Comparator.comparing(Activity::getStartDateTime));
            }

            List<File> fileList = Files.list(new File(userDir, dataDirName).toPath()).map(Path::toFile).toList();

            List<List<TrackPoint>> trackPointListList = fileList.stream()
                                                                .parallel()
                                                                .filter(file -> {
                                                                    try {
                                                                        return Files.lines(file.toPath())
                                                                                    .count() < 2506;
                                                                    } catch (IOException e) {
                                                                        e.printStackTrace();
                                                                        throw new RuntimeException();
                                                                    }
                                                                })
                                                                .map(this::parsePltFile)
                                                                .sorted(Comparator.comparing(o -> o.get(0)
                                                                                                   .getDateTime()))
                                                                .collect(Collectors.toCollection(ArrayList::new));

            List<Activity> validActivities = new ArrayList<>();

            // approch A merge the lists and seek-find the activities
            if (activities.size() > 0) {

                Collections.reverse(activities);
                for (List<TrackPoint> trackPointList : trackPointListList) {
                    int cursorPos = 0;
                    int actSize   = activities.size();
                    for (int n = actSize - 1; n >= 0; n--) {
                        Activity activity = activities.get(n);

                        if (trackPointList.size() - 1 == cursorPos) {
                            break;
                        }

                        // test if the activity matches perfectly
                        int cutFrom        = - 1;
                        int cutTo          = - 1;
                        int trackPointSize = trackPointList.size();
                        for (int i = cursorPos; i < trackPointSize; i++) {
                            if (trackPointList.get(i).getDateTime().compareTo(activity.getEndDateTime()) > 0) {
                                // if the end time of the target is before the current point stop
                                break;
                            }
                            if (cutFrom == - 1) {
                                if (trackPointList.get(i).getDateTime().equals(activity.getStartDateTime())) {
                                    cutFrom = i;
                                }
                            } else {
                                if (trackPointList.get(i).getDateTime().equals(activity.getEndDateTime())) {
                                    cutTo = i;
                                    break;
                                }
                            }
                        }

                        // found a section to cut
                        if (cutFrom != - 1 && cutTo != - 1) {
                            if (cutFrom != cursorPos) {

                                List<TrackPoint> actPoints = trackPointList.subList(cursorPos, cutFrom);
                                Activity         miniAct   = new Activity();
                                miniAct.setStartDateTime(actPoints.get(0).getDateTime());
                                miniAct.setEndDateTime(actPoints.get(actPoints.size() - 1).getDateTime());
                                miniAct.setTrackPoints(actPoints);
                                validActivities.add(miniAct);
                            }
                            List<TrackPoint> actPoints = trackPointList.subList(cutFrom, cutTo);
                            activity.setTrackPoints(actPoints);
                            validActivities.add(activity);
                            activities.remove(activity);
                            cursorPos = cutTo;
                        }
                    }
                    // the list is either iterated through or no more labels can be fit

                    if (cursorPos != trackPointList.size() - 1) {
                        Activity activity = new Activity();
                        activity.setStartDateTime(trackPointList.get(cursorPos).getDateTime());
                        activity.setEndDateTime(trackPointList.get(trackPointList.size() - 1).getDateTime());
                        activity.setTrackPoints(trackPointList.subList(cursorPos, trackPointList.size() - 1));
                        validActivities.add(activity);
                    }


                }
            } else {
                for (List<TrackPoint> trackPointList : trackPointListList) {
                    Activity activity = new Activity();
                    activity.setStartDateTime(trackPointList.get(0).getDateTime());
                    activity.setEndDateTime(trackPointList.get(trackPointList.size() - 1).getDateTime());
                    activity.setTrackPoints(trackPointList);
                    validActivities.add(activity);
                }
            }


            user.setActivities(validActivities);
            return user;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    private List<TrackPoint> parsePltFile(File pltFile) {
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
                    altitude = (int) Math.floor(Double.parseDouble(parts[3]));
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
            return points;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

}
