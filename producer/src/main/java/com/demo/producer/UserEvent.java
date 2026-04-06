package com.demo.producer;

// Shared data model — also copied into consumer-mvc and consumer-reactive
// Java 21 record: immutable, auto-generates constructor, equals, hashCode, toString
public record UserEvent(String id, String name, String email) {}
