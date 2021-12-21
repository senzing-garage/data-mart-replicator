package com.senzing.datamart;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Provides build information for the replicator.
 */
public class BuildInfo {
  /**
   * The Maven build version of the replicator.
   */
  public static final String MAVEN_VERSION;

  static {
    String resource = "/com/senzing/datamart/build-info.properties";
    String version = "UNKNOWN";
    try (InputStream is = BuildInfo.class.getResourceAsStream(resource))
    {
      Properties buildProps = new Properties();
      buildProps.load(is);
      version = buildProps.getProperty("Maven-Version");

    } catch (IOException e) {
      System.err.println("FAILED TO READ " + resource + " FILE");
      e.printStackTrace();

    } catch (Exception e) {
      e.printStackTrace();
      throw new ExceptionInInitializerError(e);

    } finally {
      MAVEN_VERSION = version;
    }
  }

  /**
   * Private default constructor.
   */
  private BuildInfo() {
    // do nothing
  }
}
