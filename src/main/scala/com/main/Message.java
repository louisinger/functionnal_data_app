package com.main;

public class Message {
    private String droneName;
    private String value;
    private Boolean isIncident;
    private String simulationName;

    public Message() {
        super();
    }

    public Message(String simulationName, String droneName, String value, Boolean isIncident) {
        this.simulationName = simulationName;
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

    public String getSimulationName() {
        return simulationName;
    }

    public void setSimulationName(String simulationName) {
        this.simulationName = simulationName;
    }

    @Override
    public String toString() {
        return "Message{" +
                "simulationName'" + simulationName + '\'' +
                ", droneName='" + droneName + '\'' +
                ", value='" + value + '\'' +
                ", isIncident=" + isIncident +
                '}';
    }
}
