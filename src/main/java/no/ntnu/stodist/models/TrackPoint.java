package no.ntnu.stodist.models;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class TrackPoint {
    private int id;
    private double latitude;
    private double longitude;
    private int altitude;
    private double dateDays;
    private LocalDateTime dateTime;
}
