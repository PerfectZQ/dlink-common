package com.sensetime.bigdata.tool;


import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class DateUtils {

    public static DateTimeFormatter[] getDateTimeFormatters() {
        return dateTimeFormatters;
    }

    public static DateTimeFormatter[] getDateFormatters() {
        return dateFormatters;
    }

    private static DateTimeFormatter[] dateTimeFormatters = new DateTimeFormatter[]{
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss"),
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSS"),
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSS'Z'"),
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSS"),
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSS'Z'"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
            DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSS")
    };

    private static DateTimeFormatter[] dateFormatters = new DateTimeFormatter[]{
            DateTimeFormatter.ofPattern("dd-MMM-yyyy"), // 08-六月-2017
            DateTimeFormatter.ofPattern("yyyy.MM.dd")
    };

    public static LocalDateTime parse(String str) throws DateTimeParseException {
        if (str == null || str.isEmpty()) {
            return null;
        }
        LocalDateTime dateTime = null;
        try {
            long timestamp = Long.parseLong(str);
            Instant instant = Instant.ofEpochMilli(timestamp);
            ZoneId zone = ZoneId.systemDefault();
            dateTime = LocalDateTime.ofInstant(instant, zone);
        } catch (NumberFormatException e) {
            // Ignore exception
        }
        if (dateTime == null) {
            for (DateTimeFormatter formatter : dateTimeFormatters) {
                try {
                    dateTime = LocalDateTime.parse(str, formatter);
                    break;
                } catch (DateTimeParseException e) {
                    // Ignore exception
                }
            }
        }
        if (dateTime == null) {
            for (DateTimeFormatter formatter : dateFormatters) {
                try {
                    dateTime = LocalDate.parse(str, formatter).atStartOfDay();
                    break;
                } catch (DateTimeParseException e) {
                    // Ignore exception
                }
            }
        }
        if (dateTime == null) {
            throw new IllegalArgumentException("Unavailable date format: " + str);
        }
        return dateTime;
    }

    public static void main(String[] args) {
        DateUtils.parse("2020-02-27T04:58:01.262Z");
    }

}
