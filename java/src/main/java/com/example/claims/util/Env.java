package com.example.claims.util;

/** Environment lookups. Only ever called from worker/bridge bootstrap or activities, never from workflow code. */
public final class Env {
  private Env() {}

  public static String get(String name, String fallback) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? fallback : value;
  }

  public static int getInt(String name, int fallback) {
    return Integer.parseInt(get(name, Integer.toString(fallback)));
  }

  public static boolean getBool(String name, boolean fallback) {
    return Boolean.parseBoolean(get(name, Boolean.toString(fallback)));
  }
}
