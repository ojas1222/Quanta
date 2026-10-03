package com.quanta.protocol;

public class Message {

    private String topic;
    private String value;

    public Message(String topic, String value) {
        this.topic = topic;
        this.value = value;
    }

    public String getTopic() {
        return topic;
    }

    public String getValue() {
        return value;
    }
}