package com.alex.space.hadoop.utils;

import com.alex.space.common.utils.CommonUtils;
import com.alex.space.hadoop.config.HadoopConstants;
import com.alex.space.hadoop.hdfs.HDFS;
import com.alex.space.hadoop.hdfs.HdfsWriter;
import com.google.common.io.ByteStreams;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * Hdfs Utils
 *
 * @author Alex Created by Alex on 2018/6/6.
 */
@Slf4j
public class HdfsUtils {

  public static String readHdfsFile(String file) throws Exception {
    InputStream is = null;
    try {
      is = HDFS.open(file);
      byte[] relationBytes = ByteStreams.toByteArray(is);
      return new String(relationBytes);
    } finally {
      CommonUtils.close(is);
    }
  }

  public static String readHdfsFileFolder(String fileFolder, boolean recursive) throws Exception {
    List<Path> files = HDFS.loadFileList(fileFolder, recursive);

    StringBuffer fileContent = new StringBuffer();

    for (Path file : files) {
      fileContent.append(readHdfsFile(file.toString()));
    }

    return fileContent.toString();
  }

  public static void writeHdfs(List<?> offsets, String filePath) throws Exception {
    HdfsWriter hw = null;
    try {
      hw = new HdfsWriter(filePath);
      StringBuilder line = new StringBuilder();
      if (offsets != null && offsets.size() > 0) {
        hw.write(offsets.get(0).toString().getBytes());
        offsets.remove(0);
        for (Object offset : offsets) {
          line.append("\n").append(offset);
          hw.write(line.toString().getBytes());
          line.delete(0, line.length());
        }
      }
    } finally {
      CommonUtils.close(hw);
    }
  }


  public static void writeHdfs(Iterator<Integer> offsets, String filePath) throws Exception {
    HdfsWriter hw = null;
    try {
      hw = new HdfsWriter(filePath);
      StringBuilder line = new StringBuilder();
      if (offsets != null && offsets.hasNext()) {
        hw.write(offsets.next().toString().getBytes());
        while (offsets.hasNext()) {
          line.append("\n").append(offsets.next());
          hw.write(line.toString().getBytes());
          line.delete(0, line.length());
        }
        hw.close();
      }
    } finally {
      CommonUtils.close(hw);
    }
  }

  public static void writeHdfs(String filePath) throws Exception {
    HdfsWriter hw = null;
    try {
      hw = new HdfsWriter(filePath);
    } finally {
      CommonUtils.close(hw);
    }
  }

  public static void writeString(String content, String filePath)
      throws Exception {
    HdfsWriter hw = null;
    try {
      hw = new HdfsWriter(filePath);
      hw.write(content.getBytes());
      hw.close();
    } finally {
      CommonUtils.close(hw);
    }
  }

  public static void writeFile(String localFilePath, String hdfsFilePath) throws Exception {
    HdfsWriter hw = null;
    try {
      hw = new HdfsWriter(hdfsFilePath);
      InputStream is = new FileInputStream(new File(localFilePath));
      hw.write(is, true);
    } finally {
      CommonUtils.close(hw);
    }
  }

  public static void readFile(String hdfsFilePath, String localFilePath) throws Exception {
    InputStream is = null;
    try {
      is = HDFS.open(hdfsFilePath);
      OutputStream os = new FileOutputStream(new File(localFilePath));
      IOUtils.copyBytes(is, os, HadoopConstants.BUFFER_SIZE, true);
    } finally {
      CommonUtils.close(is);

    }
  }

  public static void readFile(String file, OutputStream os) throws Exception {
    InputStream is = null;
    try {
      is = HDFS.open(file);
      IOUtils.copyBytes(is, os, HadoopConstants.BUFFER_SIZE, true);
    } finally {
      CommonUtils.close(is);
    }
  }

  public static void appendUserToHdfs(List<Integer> offsets, String pipelineId, String crowdId,
      String filePath) {
    OutputStream os = null;
    try {
      boolean exists = HDFS.exists(filePath);
      os = HDFS.getInstance().append(new Path(filePath));

      StringBuilder line = new StringBuilder();
      if (offsets != null && offsets.size() > 0) {

        if (!exists) {
          String first = pipelineId + "\t" + crowdId + "\t" + offsets.get(0);
          os.write((first).toString().getBytes());
          offsets.remove(0);
        }

        for (Object offset : offsets) {
          line.append("\n").append(pipelineId);
          line.append("\t").append(crowdId);
          line.append("\t").append(offset);
          os.write(line.toString().getBytes());
          line.delete(0, line.length());
        }
      }
      os.flush();
    } catch (IOException e) {
      log.error(String
              .format("content size: {} to file: {} error, exception: ", offsets.size() + 1, filePath),
          e);
    } finally {
      CommonUtils.close(os);
    }
  }

  /**
   * Test Hdfs util
   */
  public static void main(String[] args) throws Exception {
    String content = HdfsUtils.readHdfsFileFolder(args[0], true);
    log.info("=======" + content);
  }
}
