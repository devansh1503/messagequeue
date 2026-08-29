package com.devansh.messagequeue.message;

public enum AckMode {
    ZERO,
    ONE,
    ALL;

    public static AckMode from(String value){
        return switch (value.toLowerCase()) {
            case "0" -> ZERO;
            case "1" -> ONE;
            case "all" -> ALL;
            default -> throw new IllegalArgumentException("Unknown ack mode: " + value);
        };
    }
}
