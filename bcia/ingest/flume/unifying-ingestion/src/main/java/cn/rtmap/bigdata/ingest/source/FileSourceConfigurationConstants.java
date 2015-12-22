/*
 * This file is distributed under the same license as Apache Flume itself.
 *   http://www.apache.org/licenses/LICENSE-2.0
 * See the NOTICE file for copyright information.
 */
package cn.rtmap.bigdata.ingest.source;

public final class FileSourceConfigurationConstants {

  public static final String  CONFIG_INCOMING_DIR = "incoming.dir";
  public static final String  CONFIG_OUTGOING_DIR = "outgoing.dir";
  public static final String  CONFIG_BACKUP_DIR   = "backup.dir";

  // Event delimiter.  When character is encountered
  // in the incoming data stream, a new event will be
  // emitted.
  public static final String  CONFIG_DELIMITER  = "delimiter";
  public static final String  DEFAULT_DELIMITER = "\n";

  public static final String  CONFIG_VERF_EXTENSION  = "verf.extension";
  public static final String  DEFAULT_VERF_EXTENSION = ".verf";

  private FileSourceConfigurationConstants() {
    // Disable explicit creation of objects.
  }

}
