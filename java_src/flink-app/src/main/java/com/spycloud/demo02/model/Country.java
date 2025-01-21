package com.spycloud.demo02.model;

public  class Country {
    public String countryCode;
    public String countryName;
    public long population;

    // public Country() {}

    public Country(String countryCode, String countryName, long population) {
        this.countryCode = countryCode;
        this.countryName = countryName;
        this.population = population;
    }
    @Override
    public String toString() {
        return "Country{" + countryCode + "=" + countryName + ", pop=" + population + "}";
    }
}
