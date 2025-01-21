package com.spycloud.demo02.model;




public  class EnrichedUserEvent {
    public String userId;
    public String countryCode;
    public String countryName;
    public long countryPopulation;

    public EnrichedUserEvent() {}

    public EnrichedUserEvent(String userId, String countryCode, String countryName, long countryPopulation) {
        this.userId = userId;
        this.countryCode = countryCode;
        this.countryName = countryName;
        this.countryPopulation = countryPopulation;
    }
    @Override
    public String toString() {
        return "EnrichedUserEvent{userId='" + userId + "', country='" + countryName +
                "', pop=" + countryPopulation + "}";
    }
}
