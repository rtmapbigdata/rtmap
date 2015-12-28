package cn.rtmap.bigdata.ingest.agent;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

//import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

public class AgentUtils {
    private static final Logger LOG = Logger.getLogger(AgentUtils.class);

    public static void deleteFileOrDir(String dir) {
        if (dir == null) {
            LOG.error("dir is null ...");
            return;
        }
        File path = new File(dir);
        if (!path.exists()) {
            LOG.error("dir is not exit ..");
        }
        for (File file : path.listFiles()) {
            file.delete();
        }
        path.delete();
    }

    public static void deleteCsvFiles(String dir) {
        if (dir == null) {
            return;
        }
        File path = new File(dir);
        if (!path.exists()) {
            LOG.error("dir is not exit ..");
        }
        for (File file : path.listFiles()) {
            if (file.getName().endsWith(".zip")) {
                continue;
            }
            file.delete();
        }
    }
}
