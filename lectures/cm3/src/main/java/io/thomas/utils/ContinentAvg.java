package io.thomas.utils;

public class ContinentAvg {
    public String continent;
    public double average;

    public ContinentAvg() {}

    public ContinentAvg(String continent, double average) {
        this.continent = continent;
        this.average = average;
    }
    
    public String toString() {
        return continent + ": " + average;
    }
}

