package no.ntnu.stodist.models;

import lombok.Data;

import java.util.List;

@Data
public class Users {
    private int id;
    private boolean hasLabels;
    private List<Activity> activities;
}
