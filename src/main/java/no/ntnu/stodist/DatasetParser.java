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

    /**
     * Pares the geo life dataset in the provided path
     *
     * @param rootDir the root dir where the dataset is stored
     *
     * @return A list of the users parsed from the dataset.
     * @throws IOException
     */
    public List<User> parseDataset(File rootDir) throws IOException {
        ArrayList<User> retList   = new ArrayList<>();
        var             usersDirs = Files.list(rootDir.toPath()).sorted().toList();

        for (Path path : usersDirs) {
            retList.add(this.parseUserDir(path.toFile()));
        }
        return retList;


    }

    /**
     * Parses the dir containing the plots for a single user
     *
     * @param userDir the dir containing the data to parse
     *
     * @return The {@link User} whose data is stored in the provided dir
     */
    public User parseUserDir(File userDir) {
        try {
            User user = new User();
            user.setHasLabels(false);

            File           labelsFile = new File(userDir, labelFileName);
            List<Activity> activities = new ArrayList<>();

            // if there is a label file we need to scan it
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

            // Parse the tracking point files in parallel
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
                                                                                                   .getDateTime())) // they may be out of order, so we need to sort them
                                                                .collect(Collectors.toCollection(ArrayList::new));

            List<Activity> validActivities = new ArrayList<>();

            if (activities.size() > 0) {

                // we reverse the activities list, so we can safely iterate in reverse while eliminating of the same list.
                Collections.reverse(activities);
                for (List<TrackPoint> trackPointList : trackPointListList) {
                    int cursorPos = 0;
                    int actSize   = activities.size();
                    for (int n = actSize - 1; n >= 0; n--) {
                        Activity activity = activities.get(n);

                        // if we have used all the data in the plt file continue to the next one
                        if (trackPointList.size() - 1 == cursorPos) {
                            break;
                        }

                        int cutFrom        = - 1;
                        int cutTo          = - 1;
                        int trackPointSize = trackPointList.size();
                        for (int i = cursorPos; i < trackPointSize; i++) {
                            if (trackPointList.get(i).getDateTime().compareTo(activity.getEndDateTime()) > 0) {
                                // if the end time of the target activity is before the current point there is no
                                //      point to continue iterating, so we continue to the next activity
                                break;
                            }
                            if (cutFrom == - 1) {
                                if (trackPointList.get(i).getDateTime().equals(activity.getStartDateTime())) {
                                    // fond a exact match for start point. We now look for the end point
                                    cutFrom = i;
                                }
                            } else {
                                if (trackPointList.get(i).getDateTime().equals(activity.getEndDateTime())) {
                                    cutTo = i;
                                    // end point is fond stop searching for this activity
                                    break;
                                }
                            }
                        }


                        if (cutFrom != - 1 && cutTo != - 1) {
                            // we have found some secton that matches a label exactly
                            if (cutFrom != cursorPos) {
                                // if the cut is not from the start pos there is an unlabeled section before the one to cut
                                //      because the activities are sorted according to start date we know there are
                                //      no label for this entry.
                                List<TrackPoint> actPoints = trackPointList.subList(cursorPos, cutFrom);
                                Activity         miniAct   = new Activity();
                                miniAct.setStartDateTime(actPoints.get(0).getDateTime());
                                miniAct.setEndDateTime(actPoints.get(actPoints.size() - 1).getDateTime());
                                miniAct.setTrackPoints(actPoints);
                                validActivities.add(miniAct);
                            }
                            // snip the marked label zone and add it to the valid activities list.
                            List<TrackPoint> actPoints = trackPointList.subList(cutFrom, cutTo);
                            activity.setTrackPoints(actPoints);
                            validActivities.add(activity);
                            activities.remove(activity);
                            cursorPos = cutTo; // move the cursor so we don't scan this aria again
                        }
                    }
                    // the list is either iterated through or no more labels can be fit to the plt file

                    if (cursorPos != trackPointList.size()) {
                        // if we haven't scanned the entire file add the un scanned aria as a new activity.
                        Activity activity = new Activity();
                        activity.setStartDateTime(trackPointList.get(cursorPos).getDateTime());
                        activity.setEndDateTime(trackPointList.get(trackPointList.size() - 1).getDateTime());
                        activity.setTrackPoints(trackPointList.subList(cursorPos, trackPointList.size() - 1));
                        validActivities.add(activity);
                    }
                }
            } else {
                // if there are no label file simply add an activity for each plt file.
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

    /**
     * Scans the provided plt file and returns a list with its track points.
     *
     * @param pltFile the file to scan.
     *
     * @return a list with the track points.
     */
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
