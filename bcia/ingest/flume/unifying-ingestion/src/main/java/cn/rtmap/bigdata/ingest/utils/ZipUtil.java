package cn.rtmap.bigdata.ingest.utils;

import java.io.*;
import java.util.*;
import java.util.zip.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZipUtil {
	static final Logger LOG = LoggerFactory.getLogger(ZipUtil.class);
	static final int BUFFER = 2048;

	public static void unzip (String zipFile, String destPath, boolean deleteZip) {
		try {
			BufferedOutputStream dest = null;
			BufferedInputStream is = null;

			ZipEntry entry;
			ZipFile zipfile = new ZipFile(zipFile);
			Enumeration e = zipfile.entries();

			while(e.hasMoreElements()) {
				entry = (ZipEntry) e.nextElement();
				LOG.debug("Extracting: " +entry);
				is = new BufferedInputStream(zipfile.getInputStream(entry));
				int count;
				byte data[] = new byte[BUFFER];
				FileOutputStream fos = new FileOutputStream(destPath + File.separator + entry.getName());
				dest = new BufferedOutputStream(fos, BUFFER);
				while ((count = is.read(data, 0, BUFFER)) != -1) {
					dest.write(data, 0, count);
				}
				dest.flush();
				dest.close();
				is.close();
			}
			zipfile.close();
			if (deleteZip) {
				new File(zipFile).delete();
			}
		} catch(Exception e) {
			LOG.error("unzip file failed", e);
			e.printStackTrace();
		}
	}
}
