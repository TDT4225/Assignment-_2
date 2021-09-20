package no.ntnu.stodist.models;

import lombok.Data;

import java.time.LocalDateTime;
import java.util.List;

@Data
public class Activity {
    private int id;
    private List<TrackPoint> trackPoints;
    private String transportationMode;
    private LocalDateTime startDateTime;
    private LocalDateTime endDateTime;
}
