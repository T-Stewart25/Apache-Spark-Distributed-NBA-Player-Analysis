package org.example;

public class Config {
    private String computerName;
    private int port;
    private String sqliteFilePath;
    private String csvFilePath;

    public Config() {
        // Default values
        this.computerName = "baghdad.cs.colostate.edu";
        this.port = 30276;
        this.sqliteFilePath = "nba/nba.sqlite";
        this.csvFilePath = "/csv/play_by_play.csv";
    }

    public String getComputerName() {
        return computerName;
    }

    public void setComputerName(String computerName) {
        this.computerName = computerName;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getSqliteFilePath() {
        return sqliteFilePath;
    }

    public void setSqliteFilePath(String sqliteFilePath) {
        this.sqliteFilePath = sqliteFilePath;
    }

    public String getCsvFilePath() {
        return csvFilePath;
    }

    public void setCsvFilePath(String csvFilePath) {
        this.csvFilePath = csvFilePath;
    }
}
