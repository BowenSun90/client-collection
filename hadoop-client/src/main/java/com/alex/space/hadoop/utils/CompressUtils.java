package com.alex.space.hadoop.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

/**
 * Compress Utils
 *
 * @author Alex Created by Alex on 2018/6/6.
 */
public class CompressUtils {

  public final static String GZIP = ".gz";

  public final static String BZIP2 = ".bz2";

  /**
   * decompress stream
   */
  public static InputStream decompressStream(InputStream is, String ext) {
    InputStream in = null;
    try {
      if (GZIP.equalsIgnoreCase(ext)) {
        in = new GZIPInputStream(is);
      } else if (BZIP2.equalsIgnoreCase(ext)) {
        in = new BZip2CompressorInputStream(is);
      }
      return in;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * compress stream
   */
  public static OutputStream compressStream(OutputStream os, String ext) {
    OutputStream out = null;
    try {
      if (GZIP.equalsIgnoreCase(ext)) {
        out = new GZIPOutputStream(os);
      } else if (BZIP2.equalsIgnoreCase(ext)) {
        out = new BZip2CompressorOutputStream(os);
      }
      return out;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }
}
