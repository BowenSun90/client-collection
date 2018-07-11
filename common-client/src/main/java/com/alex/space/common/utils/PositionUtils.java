package com.alex.space.common.utils;

/**
 * Position Transfer Utils
 *
 * @author Alex Created by Alex on 2018/6/27.
 */
public class PositionUtils {

  public static final String BAIDU_LBS_TYPE = "bd09ll";
  public static double pi = 3.1415926535897932384626;
  public static double a = 6378245.0;
  public static double ee = 0.00669342162296594323;

  /**
   * 84 to 火星坐标系 (GCJ-02) World Geodetic System ==> Mars Geodetic System
   */
  public static Gps gps84_To_Gcj02(double lat, double lon) {
    if (!outOfChina(lat, lon)) {
      return null;
    }
    double dLat = transformLat(lon - 105.0, lat - 35.0);
    double dLon = transformLon(lon - 105.0, lat - 35.0);
    double radLat = lat / 180.0 * pi;
    double magic = Math.sin(radLat);
    magic = 1 - ee * magic * magic;
    double sqrtMagic = Math.sqrt(magic);
    dLat = (dLat * 180.0) / ((a * (1 - ee)) / (magic * sqrtMagic) * pi);
    dLon = (dLon * 180.0) / (a / sqrtMagic * Math.cos(radLat) * pi);
    double mgLat = lat + dLat;
    double mgLon = lon + dLon;
    return new Gps(mgLat, mgLon);
  }

  /**
   * * 火星坐标系 (GCJ-02) to 84 * *
   */
  public static Gps gcj_To_Gps84(double lat, double lon) {
    Gps gps = transform(lat, lon);
    double lontitude = lon * 2 - gps.getWgLon();
    double latitude = lat * 2 - gps.getWgLat();
    return new Gps(latitude, lontitude);
  }

  /**
   * 火星坐标系 (GCJ-02) 与百度坐标系 (BD-09) 的转换算法 将 GCJ-02 坐标转换成 BD-09 坐标
   */
  public static Gps gcj02_To_Bd09(double gg_lat, double gg_lon) {
    double x = gg_lon, y = gg_lat;
    double z = Math.sqrt(x * x + y * y) + 0.00002 * Math.sin(y * pi);
    double theta = Math.atan2(y, x) + 0.000003 * Math.cos(x * pi);
    double bd_lon = z * Math.cos(theta) + 0.0065;
    double bd_lat = z * Math.sin(theta) + 0.006;
    return new Gps(bd_lat, bd_lon);
  }

  /**
   * * 火星坐标系 (GCJ-02) 与百度坐标系 (BD-09) 的转换算法 * * 将 BD-09 坐标转换成GCJ-02 坐标
   */
  public static Gps bd09_To_Gcj02(double bd_lat, double bd_lon) {
    double x = bd_lon - 0.0065, y = bd_lat - 0.006;
    double z = Math.sqrt(x * x + y * y) - 0.00002 * Math.sin(y * pi);
    double theta = Math.atan2(y, x) - 0.000003 * Math.cos(x * pi);
    double gg_lon = z * Math.cos(theta);
    double gg_lat = z * Math.sin(theta);
    return new Gps(gg_lat, gg_lon);
  }

  /**
   * (BD-09)-->84
   */
  public static Gps bd09_To_Gps84(double bd_lat, double bd_lon) {
    Gps gcj02 = PositionUtils.bd09_To_Gcj02(bd_lat, bd_lon);
    Gps map84 = PositionUtils.gcj_To_Gps84(gcj02.getWgLat(),
        gcj02.getWgLon());
    return map84;
  }

  /**
   * 84-->(BD-09)
   */
  public static Gps Gps84_To_bd09(double wgs84_lat, double wgs84_lon) {
    Gps gcj02 = PositionUtils.gps84_To_Gcj02(wgs84_lat, wgs84_lon);
    Gps bd09 = PositionUtils.gcj02_To_Bd09(gcj02.getWgLat(),
        gcj02.getWgLon());
    return bd09;
  }

  public static boolean outOfChina(double lat, double lon) {
    if (lon < 72.004 || lon > 137.8347) {
      return true;
    }
    if (lat < 0.8293 || lat > 55.8271) {
      return true;
    }
    return false;
  }

  public static Gps transform(double lat, double lon) {
    if (outOfChina(lat, lon)) {
      return new Gps(lat, lon);
    }
    double dLat = transformLat(lon - 105.0, lat - 35.0);
    double dLon = transformLon(lon - 105.0, lat - 35.0);
    double radLat = lat / 180.0 * pi;
    double magic = Math.sin(radLat);
    magic = 1 - ee * magic * magic;
    double sqrtMagic = Math.sqrt(magic);
    dLat = (dLat * 180.0) / ((a * (1 - ee)) / (magic * sqrtMagic) * pi);
    dLon = (dLon * 180.0) / (a / sqrtMagic * Math.cos(radLat) * pi);
    double mgLat = lat + dLat;
    double mgLon = lon + dLon;
    return new Gps(mgLat, mgLon);
  }

  public static double transformLat(double x, double y) {
    double ret = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y
        + 0.2 * Math.sqrt(Math.abs(x));
    ret += (20.0 * Math.sin(6.0 * x * pi) + 20.0 * Math.sin(2.0 * x * pi)) * 2.0 / 3.0;
    ret += (20.0 * Math.sin(y * pi) + 40.0 * Math.sin(y / 3.0 * pi)) * 2.0 / 3.0;
    ret += (160.0 * Math.sin(y / 12.0 * pi) + 320 * Math.sin(y * pi / 30.0)) * 2.0 / 3.0;
    return ret;
  }

  public static double transformLon(double x, double y) {
    double ret = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1
        * Math.sqrt(Math.abs(x));
    ret += (20.0 * Math.sin(6.0 * x * pi) + 20.0 * Math.sin(2.0 * x * pi)) * 2.0 / 3.0;
    ret += (20.0 * Math.sin(x * pi) + 40.0 * Math.sin(x / 3.0 * pi)) * 2.0 / 3.0;
    ret += (150.0 * Math.sin(x / 12.0 * pi) + 300.0 * Math.sin(x / 30.0
        * pi)) * 2.0 / 3.0;
    return ret;
  }

  private static final double EARTH_RADIUS = 6378137;//赤道半径(单位m)

  /**
   * 转化为弧度(rad)
   */
  private static double rad(double d) {
    return d * Math.PI / 180.0;
  }

  /**
   * 基于余弦定理求两经纬度距离,返回的距离，单位m
   */
  public static double lantitudeLongitudeDist(double lon1, double lat1, double lon2, double lat2) {
    double radLat1 = rad(lat1);
    double radLat2 = rad(lat2);
    double radLon1 = rad(lon1);
    double radLon2 = rad(lon2);
    if (radLat1 < 0) {
      radLat1 = Math.PI / 2 + Math.abs(radLat1);// south
    }
    if (radLat1 > 0) {
      radLat1 = Math.PI / 2 - Math.abs(radLat1);// north
    }
    if (radLon1 < 0) {
      radLon1 = Math.PI * 2 - Math.abs(radLon1);// west
    }
    if (radLat2 < 0) {
      radLat2 = Math.PI / 2 + Math.abs(radLat2);// south
    }
    if (radLat2 > 0) {
      radLat2 = Math.PI / 2 - Math.abs(radLat2);// north
    }
    if (radLon2 < 0) {
      radLon2 = Math.PI * 2 - Math.abs(radLon2);// west
    }
    double x1 = EARTH_RADIUS * Math.cos(radLon1) * Math.sin(radLat1);
    double y1 = EARTH_RADIUS * Math.sin(radLon1) * Math.sin(radLat1);
    double z1 = EARTH_RADIUS * Math.cos(radLat1);
    double x2 = EARTH_RADIUS * Math.cos(radLon2) * Math.sin(radLat2);
    double y2 = EARTH_RADIUS * Math.sin(radLon2) * Math.sin(radLat2);
    double z2 = EARTH_RADIUS * Math.cos(radLat2);
    double d = Math.sqrt((x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2) + (z1 - z2) * (z1 - z2));
    //余弦定理求夹角
    double theta = Math.acos(
        (EARTH_RADIUS * EARTH_RADIUS + EARTH_RADIUS * EARTH_RADIUS - d * d) / (2 * EARTH_RADIUS
            * EARTH_RADIUS));
    double dist = theta * EARTH_RADIUS;
    return dist;
  }

  /**
   * 距离M
   */
  public static double getDistance(double lat1, double lng1, double lat2, double lng2) {
    double radLat1 = rad(lat1);
    double radLat2 = rad(lat2);
    double a = radLat1 - radLat2;
    double b = rad(lng1) - rad(lng2);
    double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) +
        Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)));
    s = s * EARTH_RADIUS;
    s = Math.round(s * 1000);
    return s;
  }

  public static class Gps {

    private double wgLat;
    private double wgLon;

    public Gps(double wgLat, double wgLon) {
      setWgLat(wgLat);
      setWgLon(wgLon);
    }

    public double getWgLat() {
      return wgLat;
    }

    public void setWgLat(double wgLat) {
      this.wgLat = wgLat;
    }

    public double getWgLon() {
      return wgLon;
    }

    public void setWgLon(double wgLon) {
      this.wgLon = wgLon;
    }

    @Override
    public String toString() {
      return wgLat + "," + wgLon;
    }
  }
}