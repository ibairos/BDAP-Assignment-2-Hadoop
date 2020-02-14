package common;

public class DistanceUtils {

    public static double flatSurfaceDistance(double startLat, double startLong, double endLat, double endLong) {
        double degreesToRadians = Math.PI/180;

        double deltaLat = endLat - startLat;
        double deltaLong = endLong - startLong;
        double meanLat = degreesToRadians * ((startLat + endLat) / 2);

        double k1 = 111.13209 - 0.56605 * Math.cos(2 * meanLat) + 0.00120 * Math.cos(4 * meanLat);
        double k2 = 111.41513 * Math.cos(meanLat) - 0.09455 * Math.cos(3 * meanLat) + 0.00012 * Math.cos(5 * meanLat);

        return Math.sqrt(Math.pow(k1 * deltaLat, 2) + Math.pow(k2 * deltaLong, 2));
    }

    public static boolean taxiInAirport(double startLat, double startLon, double endLat, double endLon) {
        double startDistance = flatSurfaceDistance(startLat, startLon, Common.AIRPORT_LAT, Common.AIRPORT_LON);
        double endDistance = flatSurfaceDistance(endLat, endLon, Common.AIRPORT_LAT, Common.AIRPORT_LON);

        return startDistance <= 1 || endDistance <= 1;
    }
}
