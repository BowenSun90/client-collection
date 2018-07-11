package com.alex.space.hbase.repository;

import com.alex.space.hbase.factory.HbaseDataAccessException;
import java.nio.charset.Charset;
import lombok.Data;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;

/**
 * Base class for {@link HBaseRepository}, defining commons properties such as {@link
 * HTableInterfaceFactory} and {@link Configuration}. <p> Not intended to be used directly.
 *
 * @author Costin Leau
 */
@Data
public abstract class HBaseAccessor {

  private String encoding;
  private Charset charset = getCharset(encoding);

  private HTableInterfaceFactory tableFactory;
  private Configuration configuration;

  protected void afterPropertiesSet() {
    if (configuration == null) {
      throw new HbaseDataAccessException("A valid configuration is required");
    }

    charset = getCharset(encoding);
  }

  private Charset getCharset(String encoding) {
    return (encoding != null && !"".equals(encoding) ? Charset.forName(encoding)
        : Charset.forName("UTF-8"));
  }
}