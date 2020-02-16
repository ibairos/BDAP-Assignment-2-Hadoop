package model;

public enum TripStatus {
    START, MIDDLE, END, NULL;

    public static TripStatus parse(String start, String end) {
        if (start.equals("E") && end.equals("M")) {
            return START;
        } else if (start.equals("M") && end.equals("M")) {
            return MIDDLE;
        } else if (start.equals("M") && end.equals("E")) {
            return END;
        } else {
            return NULL;
        }
    }
}
