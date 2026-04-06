package com.demo.mvc;

// Shared data model — same record as in producer and consumer-reactive
public record UserEvent(String id, String name, String email) {}
