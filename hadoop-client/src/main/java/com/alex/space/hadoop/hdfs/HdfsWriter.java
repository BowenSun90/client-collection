package com.alex.space.hadoop.hdfs;

import com.alex.space.hadoop.config.HadoopConstants;
import com.alex.space.hadoop.utils.CompressUtils;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * HDFS writer
 *
 * @author Alex Created by Alex on 2018/6/6.
 */
@Slf4j
public class HdfsWriter implements Closeable {

  private OutputStream os = null;

  private Compress compress = null;

  public enum Compress {
    GZIP, BZIP2
  }

  /**
   * Open hdfs writer
   *
   * @param outfile file path to write, support '.gz'
   */
  public HdfsWriter(String outfile) throws IOException {
    this(outfile, false);
  }

  /**
   * Open hdfs writer
   *
   * @param outfile file path to write, support '.gz'
   * @param ignoreCompress ignore compress
   */
  public HdfsWriter(String outfile, boolean ignoreCompress) throws IOException {
    if (HadoopConstants.EMPTY_FILE.equals(outfile)) {
      return;
    }

    int index = outfile.lastIndexOf("/");
    if (index == outfile.length() - 1) {
      throw new IOException("filePath cannot end with '/'.");
    }

    Path path = new Path(outfile);
    if (!HDFS.exists(path.getParent())) {
      HDFS.mkdirs(path.getParent());
      log.info("HDFS mkdir '" + path.getParent().toString() + "'");
    }
    if (HDFS.exists(path)) {
      throw new IOException("this file '" + outfile + "' is already exists.");
    } else {
      int point = path.toString().lastIndexOf(".");
      String ext = "";
      if (point > 0) {
        ext = path.toString().substring(point); // 截取后缀名
      }
      if (!ignoreCompress) {
        if (ext.equalsIgnoreCase(CompressUtils.GZIP)) {
          compress = Compress.GZIP;
          os = CompressUtils
              .compressStream(HDFS.create(path, true, HadoopConstants.BUFFER_SIZE), ext);
        } else if (ext.equalsIgnoreCase(CompressUtils.BZIP2)) {
          compress = Compress.BZIP2;
          os = CompressUtils.compressStream(
              HDFS.create(path, true, HadoopConstants.BUFFER_SIZE), ext);
        } else {
          os = HDFS.create(path, true, HadoopConstants.BUFFER_SIZE);
        }
      } else {
        os = HDFS.create(path, true, HadoopConstants.BUFFER_SIZE);
      }
      log.info("HDFS create file '" + path.toString() + "'");
    }
  }

  /**
   * write byte
   *
   * @param b byte to write
   */
  public void write(byte[] b) throws IOException {
    if (os == null) {
      return;
    }
    os.write(b);
  }

  /**
   * write stream
   *
   * @param is input stream
   * @param close close stream after write or not
   */
  public void write(InputStream is, boolean close) throws IOException {
    if (os == null) {
      return;
    }
    IOUtils.copyBytes(is, os, HadoopConstants.BUFFER_SIZE, close);
  }

  /**
   * close writer
   */
  @Override
  public void close() throws IOException {
    if (os != null) {
      if (compress != null) {
        switch (compress) {
          case GZIP:
            ((GZIPOutputStream) os).finish();
            break;
          case BZIP2:
            ((BZip2CompressorOutputStream) os).finish();
            break;
          default:
            break;
        }
      }
      os.close();
      os = null;
    }
  }

  /**
   * Test hdfs writer
   *
   * @param args input file path and writer file path
   */
  public static void main(String[] args) throws Exception {
    HdfsWriter hw = new HdfsWriter(args[1]);
    File file = new File(args[0]);
    hw.write(new FileInputStream(file), true);
  }
}
