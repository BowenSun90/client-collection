package com.alex.space.hadoop.hdfs;

import com.alex.space.hadoop.config.HadoopConstants;
import com.alex.space.hadoop.utils.CompressUtils;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;

/**
 * HDFS input stream
 *
 * @author Alex Created by Alex on 2018/6/6.
 */
@Slf4j
public class HdfsInputStream {

  private List<Path> files = null;
  private int index = 0;

  public HdfsInputStream(String path, boolean recursive) {
    try {
      this.files = HDFS.loadFileList(path, recursive);
    } catch (IOException e) {
      log.error("load file list error in path '" + path + "'.");
      e.printStackTrace();
    }
  }

  /**
   * Open next file stream
   */
  public InputStream next() {
    if (this.files != null && this.index < this.files.size()) {
      Path path = this.files.get(this.index);
      this.index++;
      try {
        int point = path.toString().lastIndexOf(".");
        String ext = "";
        if (point > 0) {
          // 截取后缀名
          ext = path.toString().substring(point);
        }
        if (ext.equalsIgnoreCase(CompressUtils.GZIP)
            || ext.equalsIgnoreCase(CompressUtils.BZIP2)) {
          return CompressUtils.decompressStream(
              HDFS.open(path, HadoopConstants.BUFFER_SIZE), ext);
        } else {
          return HDFS.open(path, HadoopConstants.BUFFER_SIZE);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return null;
  }

}
