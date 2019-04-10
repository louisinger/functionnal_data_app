package com.main;

public class Message {
    private String droneName;
    private String value;
    private Boolean isIncident;

    public Message() {
        super();
    }

    public Message(String droneName, String value, Boolean isIncident) {
        this.droneName = droneName;
        this.value = value;
        this.isIncident = isIncident;
    }

    public String getDroneName() {
        return droneName;
    }

    public void setDroneName(String droneName) {
        this.droneName = droneName;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Boolean getIncident() {
        return isIncident;
    }

    public void setIncident(Boolean incident) {
        isIncident = incident;
    }

    @Override
    public String toString() {
        return "Message{" +
                "droneName='" + droneName + '\'' +
                ", value='" + value + '\'' +
                ", isIncident=" + isIncident +
                '}';
    }
}
