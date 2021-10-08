package no.ntnu.stodist.models;

import lombok.Data;

import java.util.List;
import java.util.ArrayList;

@Data
public class User {
    private static int idCounter = 0;

    private static synchronized int getNextId() {
        idCounter = idCounter + 1;
        return idCounter;
    }

    public User() {
        this.id = getNextId();
    }

    private int id;
    private boolean hasLabels;
    private List<Activity> activities =  new ArrayList<>();
    
    public void addActivity (Activity activity) {
        this.activities.add(activity);
    }
}
