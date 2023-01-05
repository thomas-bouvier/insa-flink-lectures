package io.thomas.utils;

public class WorldStream {
    public String continent;
    public String country;
    public double population;
    public double percent;
    
    public WorldStream() {}

    public WorldStream(String continent, String country, double population, double percent) {
        this.continent = continent;
        this.country = country;
        this.population = population;
        this.percent = percent;
    }
}
