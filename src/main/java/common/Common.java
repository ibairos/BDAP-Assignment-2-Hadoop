package common;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.TimeZone;

public class Common {

    public static final int SEGMENT_LENGTH = 9;
    public static final double MAX_SPEED_KPH = 120;
    public static final double SEGMENT_TOLERANCE_MILLIS = 2000;
    public static final double DISTANCE_TOLERANCE_KM = 0.05;
    public static final double MAX_DISTANCE_KM = 50;


    public static final Calendar CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("America/Los_Angeles"));
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static final ZoneId ZONE_ID = ZoneId.of("America/Los_Angeles");

    public static final double AIRPORT_LAT = 37.62131;
    public static final double AIRPORT_LON = -122.37896;

}
