package com.kafka.dto;


import lombok.Data;

@Data
public class Customer {

    private int id;
    private String name;
    private String city;
    private String country;
}
