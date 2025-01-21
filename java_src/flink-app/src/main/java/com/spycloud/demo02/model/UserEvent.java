package com.spycloud.demo02.model;


public  class UserEvent {
    public String userId;
    public String countryCode;

    // Required default constructor for Flink serialization
    public UserEvent() {}

    public UserEvent(String userId, String countryCode) {
        this.userId = userId;
        this.countryCode = countryCode;
    }
    @Override
    public String toString() {
        return "UserEvent{userId='" + userId + "', countryCode='" + countryCode + "'}";
    }
}
