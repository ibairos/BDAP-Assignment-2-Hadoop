package model;

public enum TaxiStatus {
    EMPTY, FULL, NULL;

    public static TaxiStatus parse(String s) {
        switch (s) {
            case "E":
                return EMPTY;
            case "M":
                return FULL;
            default:
                return NULL;
        }
    }
}
