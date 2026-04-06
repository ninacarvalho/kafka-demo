package com.demo.reactive;

// Shared data model — same record as in producer and consumer-mvc
public record UserEvent(String id, String name, String email) {}
