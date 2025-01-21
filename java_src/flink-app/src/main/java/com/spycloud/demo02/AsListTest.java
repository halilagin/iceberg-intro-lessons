package com.spycloud.demo02;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.github.javafaker.Faker;
import com.google.gson.Gson;
import com.spycloud.demo02.BroadcastJoinInMemory.UserEvent;

public class AsListTest {
    

    private static List<String> generateFakePersons(int count) {
        Faker faker = new Faker();
        Gson gson = new Gson();
        List<String> jsonList = new ArrayList<>();
        List<UserEvent> userList  = new ArrayList<>();
        List<UserEvent> knownUserList = Arrays.asList(
            new UserEvent("user1", "US"),
            new UserEvent("user2", "DE"),
            new UserEvent("user3", "XX") // Unknown country code
        );
        userList.addAll(knownUserList);


        for (int i = 0; i < count; i++) {
            UserEvent user = new UserEvent("user"+faker.number().digit(), faker.country().countryCode2());
            userList.add(user);
        }

        userList.forEach(user -> {
            String jsonString = gson.toJson(user);
            jsonList.add(jsonString);
        });
        
        return jsonList;
    }

    public static void main(String[] args) {
        List<String> jsonList = generateFakePersons(10);
        jsonList.forEach(jsonS -> {
            System.err.println(jsonS);
        });

    }
}
