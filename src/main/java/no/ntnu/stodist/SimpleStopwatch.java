package no.ntnu.stodist;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * A simple stopwatch for timing stuff
 */
public class SimpleStopwatch {

    private static final HashMap<String, TimeRange> timeMap = new HashMap<>();
    private static final TimeUnit seconds = TimeUnit.SECONDS;
    private static final TimeUnit millis = TimeUnit.MILLISECONDS;
    private static final TimeUnit microsecond = TimeUnit.MICROSECONDS;
    private static final TimeUnit nanos = TimeUnit.NANOSECONDS;

    /**
     * start a timer with the provided id
     *
     * @param key the id of the timer
     */
    public static void start(String key) {
        TimeRange t = new TimeRange();
        t.start = System.nanoTime();

        if (timeMap.containsKey(key)) {
            System.out.printf("TIMER WITH KEY %s EXISTS\n", key);
        } else {
            timeMap.put(key, t);
        }
    }

    /**
     * Stops the timer with the given id. if display is set to true the timing info is shown
     *
     * @param key     the id of the timer
     * @param display whether or not to display the timer info
     */
    public static void stop(String key, boolean display) {
        stop(key);
        if (display) {
            show(key);
        }
    }

    /**
     * Stops the timer with the given key
     *
     * @param key the id of the timer
     */
    public static void stop(String key) {

        if (timeMap.containsKey(key)) {
            timeMap.get(key).stop = System.nanoTime();
        } else {
            System.out.printf("TIMER WITH KEY %s DOES NOT EXISTS\n", key);
        }
    }

    /**
     * Displays the timer info for the provided id, with the id as display name
     *
     * @param key the id of the timer
     */
    public static void show(String key) {
        show(key, key);
    }

    /**
     * Displays the timer info for the provided id, displays the name provided
     *
     * @param key  the id of the timer
     * @param name the name to display
     */
    public static void show(String key, String name) {
        if (timeMap.containsKey(key)) {
            TimeRange unit = timeMap.get(key);
            if (unit.stop != - 1 && unit.start != - 1) {

                long delta = unit.stop - unit.start;
                System.out.println("-----------------------------------");
                System.out.printf("Timings for block %s\n" +
                                          "in seconds : %s \n" +
                                          "in millisec: %s \n" +
                                          "in microsec: %s \n" +
                                          "in nanons  : %s \n",
                                  name,
                                  seconds.convert(delta, nanos),
                                  millis.convert(delta, nanos),
                                  microsecond.convert(delta, nanos),
                                  delta
                );
                System.out.println("-----------------------------------");

            }
        }
    }

    /**
     * DataHolder for a time range
     */
    private static class TimeRange {
        public long start = - 1;
        public long stop = - 1;
    }
}

