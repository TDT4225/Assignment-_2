package no.ntnu.stodist.models;

import lombok.Data;

import java.time.LocalDateTime;
import java.util.Optional;

@Data
public class TrackPoint {
    private static int idCounter = 0;

    private static synchronized int getNextId() {
        idCounter = idCounter + 1;
        return idCounter;
    }

    public TrackPoint() {
        this.id = getNextId();
    }

    private int id;
    private double latitude;
    private double longitude;
    private Integer altitude;
    private double dateDays;
    private LocalDateTime dateTime;


    public Optional<Integer> getAltitude() {
        return Optional.ofNullable(altitude);
    }
}
