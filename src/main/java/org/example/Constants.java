package org.example;

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

import java.util.Map;

public class Constants {

  public static final Map<String, String> stateChange = ImmutableMap.of(
      "idle", "pending",
      "pending", "plan",
      "plan", "execute",
      "execute", "commit",
      "commit", "idle"
  );
  public static final String[] states = {"idle", "pending", "plan", "execute", "commit"};
  public static final String[] tasks = {"minor", "major", "full"};

}
