package no.ntnu.stodist.models;

import lombok.Data;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Data
public class Activity {
    private static int idCounter = 0;

    private static synchronized int getNextId() {
        idCounter = idCounter + 1;
        return idCounter;
    }

    public Activity() {
        this.id = getNextId();
    }

    public Activity(int id) {
        this.id = id;
    }

    private int id;
    private List<TrackPoint> trackPoints = new ArrayList<>();
    private String transportationMode;
    private LocalDateTime startDateTime;
    private LocalDateTime endDateTime;

    public Optional<String> getTransportationMode() {
        return Optional.ofNullable(transportationMode);
    }
}
