package com.alex.space.hadoop.hdfs;

import com.alex.space.hadoop.config.HadoopConstants;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import lombok.extern.slf4j.Slf4j;

/**
 * HDFS reader
 *
 * @author Alex Created by Alex on 2018/6/6.
 */
@Slf4j
public class HdfsReader {

  private HdfsInputStream hdfsInputStream = null;
  private InputStream inputStream = null;
  private Reader reader = null;
  private BufferedReader bufferedReader = null;

  /**
   * Open hdfs reader
   *
   * @param path file path to read
   * @param recursive read the file hdfsInputStream the subdirectory or not
   */
  public HdfsReader(String path, boolean recursive) {
    log.info("create reader on '" + path + "' and recursive inputStream "
        + recursive);
    if (HadoopConstants.EMPTY_FILE.equals(path)) {
      return;
    }
    hdfsInputStream = new HdfsInputStream(path, recursive);
  }

  /**
   * read next file
   */
  private boolean nextStream() {
    close();

    if (hdfsInputStream == null) {
      return false;
    }
    inputStream = hdfsInputStream.next();

    if (inputStream == null) {
      return false;
    }
    reader = new InputStreamReader(inputStream);
    bufferedReader = new BufferedReader(reader);
    return true;
  }

  /**
   * read line
   */
  public String readLine() throws IOException {

    String line;

    if (inputStream == null) {
      if (!nextStream()) {
        return null;
      }
    }
    line = bufferedReader.readLine();
    if (line == null) {
      boolean hasNext = nextStream();
      if (hasNext) {
        return readLine();
      }
    }
    return line;
  }

  /**
   * close
   */
  public void close() {
    if (bufferedReader != null) {
      try {
        bufferedReader.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    if (reader != null) {
      try {
        reader.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    if (inputStream != null) {
      try {
        inputStream.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Test Hdfs reader
   *
   * @param args read file path
   */
  public static void main(String[] args) throws Exception {
    HdfsReader reader = new HdfsReader(args[0], true);
    System.out.println(reader.readLine());
  }
}
