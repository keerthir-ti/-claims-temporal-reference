package claims.runtime;

/** Configuration from environment variables, with defaults for local runs. */
public final class Env {
  private Env() {}

  public static String get(String name, String fallback) {
    String v = System.getenv(name);
    return v == null || v.isBlank() ? fallback : v;
  }

  public static int getInt(String name, int fallback) {
    return Integer.parseInt(get(name, Integer.toString(fallback)));
  }

  public static double getDouble(String name, double fallback) {
    return Double.parseDouble(get(name, Double.toString(fallback)));
  }

  public static boolean getBool(String name, boolean fallback) {
    return Boolean.parseBoolean(get(name, Boolean.toString(fallback)));
  }
}
