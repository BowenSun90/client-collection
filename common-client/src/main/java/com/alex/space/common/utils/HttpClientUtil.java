package com.alex.space.common.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.charset.UnsupportedCharsetException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.net.ssl.SSLHandshakeException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.protocol.ExecutionContext;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.DeserializationConfig.Feature;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

/**
 * Apache Httpclient 4.0 Utils
 *
 * @author Alex Created by Alex on 2018/7/10.
 */
@SuppressWarnings("all")
public class HttpClientUtil {

  private static final String CHARSET_UTF8 = "UTF-8";
  private static final String CHARSET_GBK = "GBK";
  private static final String SSL_DEFAULT_SCHEME = "https";
  private static final int SSL_DEFAULT_PORT = 443;

  private static final ObjectMapper mapper = new ObjectMapper();

  private static final String APPLICATION_JSON = "application/json";
  private static final String CONTENT_TYPE_TEXT_JSON = "text/json";

  // 异常自动恢复处理, 使用HttpRequestRetryHandler接口实现请求的异常恢复
  private static HttpRequestRetryHandler requestRetryHandler = new HttpRequestRetryHandler() {
    // 自定义的恢复策略
    @Override
    public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
      // 设置恢复策略，在发生异常时候将自动重试3次
      if (executionCount >= 3) {
        // Do not retry if over max retry count
        return false;
      }
      if (exception instanceof NoHttpResponseException) {
        // Retry if the server dropped connection on us
        return true;
      }
      if (exception instanceof SSLHandshakeException) {
        // Do not retry on SSL handshake exception
        return false;
      }
      HttpRequest request = (HttpRequest) context.getAttribute(ExecutionContext.HTTP_REQUEST);
      boolean idempotent = (request instanceof HttpEntityEnclosingRequest);
      if (!idempotent) {
        // Retry if the request is considered idempotent
        return true;
      }
      return false;
    }
  };

  // 使用ResponseHandler接口处理响应，HttpClient使用ResponseHandler会自动管理连接的释放，解决了对连接的释放管理
  private static ResponseHandler responseHandler = new ResponseHandler() {
    // 自定义响应处理
    @Override
    public String handleResponse(HttpResponse response)
        throws ClientProtocolException, IOException {
      HttpEntity entity = response.getEntity();
      if (!(response.getStatusLine().getStatusCode() == 200
          || response.getStatusLine().getStatusCode() == 201)) {
        String errorMsg = "Failed : HTTP error code : " + response.getStatusLine().getStatusCode();
        if (entity != null) {
          String charset =
              EntityUtils.getContentCharSet(entity) == null ? CHARSET_GBK
                  : EntityUtils.getContentCharSet(entity);
          errorMsg =
              errorMsg + "\n[response]: " + new String(EntityUtils.toByteArray(entity), charset);
        }
        throw new RuntimeException(errorMsg);
      }
      if (entity != null) {
        String charset =
            EntityUtils.getContentCharSet(entity) == null ? CHARSET_GBK
                : EntityUtils.getContentCharSet(entity);
        return new String(EntityUtils.toByteArray(entity), charset);
      } else {
        return null;
      }
    }
  };

  /**
   * Get方式提交,URL中包含查询参数, 格式：http://www.g.cn?search=p&name=s.....
   */
  public static String get(String url) {
    return get(url, new HashMap(), "");
  }

  /**
   * Get方式提交,URL中不包含查询参数, 格式：http://www.g.cn
   */
  public static String get(String url, Map params) {
    return get(url, params, "");
  }

  /**
   * Get方式提交,URL中不包含查询参数, 格式：http://www.g.cn
   */
  public static String get(String url, Map params, String charset) {
    if (url == null || url.trim().length() == 0) {
      return null;
    }
    List qparams = getParamsList(params);
    if (qparams != null && qparams.size() > 0) {
      charset = (charset == null ? CHARSET_GBK : charset);
      String formatParams = URLEncodedUtils.format(qparams, charset);
      url =
          (url.indexOf("?")) < 0 ? (url + "?" + formatParams)
              : (url.substring(0, url.indexOf("?") + 1) + formatParams);
    }
    DefaultHttpClient httpclient = getDefaultHttpClient(charset);
    HttpGet hg = new HttpGet(url);
    // 发送请求，得到响应
    String responseStr = null;
    try {
      responseStr = (String) httpclient.execute(hg, responseHandler);
    } catch (ClientProtocolException e) {
      throw new RuntimeException("客户端连接协议错误", e);
    } catch (IOException e) {
      throw new RuntimeException("IO操作异常", e);
    } finally {
      abortConnection(hg, httpclient);
    }
    return responseStr;
  }

  public static <T> T get(String url, Class<T> valueType) {
    return get(url, null, null, valueType);
  }

  public static <T> T get(String url, Map params, Class<T> valueType) {
    return get(url, params, null, valueType);
  }

  public static <T> T get(String url, Map params, String charset, Class<T> valueType) {
    String value = get(url, params, charset);
    return strToObject(value, valueType);
  }

  public static <T> T get(String url, TypeReference valueTypeRef) {
    return get(url, null, null, valueTypeRef);
  }

  public static <T> T get(String url, Map params, TypeReference valueTypeRef) {
    return get(url, params, null, valueTypeRef);
  }

  public static <T> T get(String url, Map params, String charset, TypeReference valueTypeRef) {
    String value = get(url, params, charset);

    return strToObject(value, valueTypeRef);
  }

  /**
   * Post方式提交,URL中不包含提交参数, 格式：http://www.g.cn
   *
   * @param url 提交地址
   * @param params 提交参数集, 键/值对
   * @return 响应消息
   */
  public static String post(String url, Map params) {
    return post(url, params, "");
  }

  /**
   * Post方式提交,URL中不包含提交参数, 格式：http://www.g.cn
   *
   * @param url 提交地址
   * @param params 提交参数集, 键/值对
   * @param charset 参数提交编码集
   * @return 响应消息
   */
  public static String post(String url, Map params, String charset) {
    if (url == null || url.trim().length() == 0) {
      return null;
    }
    // 创建HttpClient实例
    DefaultHttpClient httpclient = getDefaultHttpClient(charset);
    UrlEncodedFormEntity formEntity = null;
    try {
      if (charset == null || charset.trim().length() == 0) {
        formEntity = new UrlEncodedFormEntity(getParamsList(params));
      } else {
        formEntity = new UrlEncodedFormEntity(getParamsList(params), charset);
      }
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("不支持的编码集", e);
    }
    HttpPost hp = new HttpPost(url);
    hp.setEntity(formEntity);
    // 发送请求，得到响应
    String responseStr = null;
    try {
      responseStr = (String) httpclient.execute(hp, responseHandler);
    } catch (ClientProtocolException e) {
      throw new RuntimeException("客户端连接协议错误", e);
    } catch (IOException e) {
      throw new RuntimeException("IO操作异常", e);
    } finally {
      abortConnection(hp, httpclient);
    }
    return responseStr;
  }

  /**
   * Post方式提交,URL中不包含提交参数, 格式：http://www.g.cn
   *
   * @param url 提交地址
   * @param body 提交参数
   * @param charset 参数提交编码集
   * @return 响应消息
   */
  public static String post(String url, String body, String charset) {
    if (url == null || url.trim().length() == 0) {
      return null;
    }
    // 创建HttpClient实例
    DefaultHttpClient httpclient = getDefaultHttpClient(charset);
    StringEntity stringEntity = new StringEntity(body, HTTP.UTF_8);
    HttpPost hp = new HttpPost(url);
    hp.setEntity(stringEntity);
    hp.addHeader("Content-Type", "application/json;charset=UTF-8");

    // 发送请求，得到响应
    String responseStr = null;
    try {
      responseStr = (String) httpclient.execute(hp, responseHandler);
    } catch (ClientProtocolException e) {
      throw new RuntimeException("客户端连接协议错误", e);
    } catch (IOException e) {
      throw new RuntimeException("IO操作异常", e);
    } catch (RuntimeException e) {
      throw new RuntimeException("post failed url: " + url + " body: " + body, e);
    } finally {
      abortConnection(hp, httpclient);
    }
    return responseStr;
  }

  /**
   * Post方式提交,忽略URL中包含的参数,解决SSL双向数字证书认证
   *
   * @param url 提交地址
   * @param params 提交参数集, 键/值对
   * @param charset 参数编码集
   * @param keystoreUrl 密钥存储库路径
   * @param keystorePassword 密钥存储库访问密码
   * @param truststoreUrl 信任存储库绝路径
   * @param truststorePassword 信任存储库访问密码, 可为null
   * @return 响应消息
   */
  public static String post(String url, Map params, String charset, final URL keystoreUrl,
      final String keystorePassword, final URL truststoreUrl,
      final String truststorePassword) {
    if (url == null || url.trim().length() == 0) {
      return null;
    }
    DefaultHttpClient httpclient = getDefaultHttpClient(charset);
    UrlEncodedFormEntity formEntity = null;
    try {
      if (charset == null || charset.trim().length() == 0) {
        formEntity = new UrlEncodedFormEntity(getParamsList(params));
      } else {
        formEntity = new UrlEncodedFormEntity(getParamsList(params), charset);
      }
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("不支持的编码集", e);
    }
    HttpPost hp = null;
    String responseStr = null;
    try {
      KeyStore keyStore = createKeyStore(keystoreUrl, keystorePassword);
      KeyStore trustStore = createKeyStore(truststoreUrl, keystorePassword);
      SSLSocketFactory socketFactory = new SSLSocketFactory(keyStore, keystorePassword, trustStore);
      Scheme scheme = new Scheme(SSL_DEFAULT_SCHEME, socketFactory, SSL_DEFAULT_PORT);
      httpclient.getConnectionManager().getSchemeRegistry().register(scheme);
      hp = new HttpPost(url);
      hp.setEntity(formEntity);
      responseStr = (String) httpclient.execute(hp, responseHandler);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("指定的加密算法不可用", e);
    } catch (KeyStoreException e) {
      throw new RuntimeException("keytore解析异常", e);
    } catch (CertificateException e) {
      throw new RuntimeException("信任证书过期或解析异常", e);
    } catch (FileNotFoundException e) {
      throw new RuntimeException("keystore文件不存在", e);
    } catch (IOException e) {
      throw new RuntimeException("I/O操作失败或中断 ", e);
    } catch (UnrecoverableKeyException e) {
      throw new RuntimeException("keystore中的密钥无法恢复异常", e);
    } catch (KeyManagementException e) {
      throw new RuntimeException("处理密钥管理的操作异常", e);
    } finally {
      abortConnection(hp, httpclient);
    }
    return responseStr;
  }

  public static <T> T post(String url, String body, Class<T> valueType) {
    String value = post(url, body, "");
    return strToObject(value, valueType);
  }

  public static <T> T post(String url, String body, String charset, Class<T> valueType) {
    String value = post(url, body, charset);
    return strToObject(value, valueType);
  }

  public static <T> T post(String url, Map params, Class<T> valueType) {
    String value = post(url, params, "");
    return strToObject(value, valueType);
  }

  public static <T> T post(String url, Map params, String charset, Class<T> valueType) {
    String value = post(url, params, charset);
    return strToObject(value, valueType);
  }

  public static <T> T post(String url, Map params, String charset, final URL keystoreUrl,
      final String keystorePassword,
      final URL truststoreUrl,
      final String truststorePassword, Class<T> valueType) {
    String value = post(url, params, charset, keystoreUrl, keystorePassword, truststoreUrl,
        truststorePassword);
    return strToObject(value, valueType);
  }

  public static <T> T post(String url, Map params, TypeReference valueTypeRef) {
    String value = post(url, params, "");
    return strToObject(value, valueTypeRef);
  }

  public static <T> T post(String url, Map params, String charset, TypeReference valueTypeRef) {
    String value = post(url, params, charset);
    return strToObject(value, valueTypeRef);
  }

  public static <T> T post(String url, Map params, String charset, final URL keystoreUrl,
      final String keystorePassword,
      final URL truststoreUrl,
      final String truststorePassword, TypeReference valueTypeRef) {
    String value = post(url, params, charset, keystoreUrl, keystorePassword, truststoreUrl,
        truststorePassword);
    return strToObject(value, valueTypeRef);
  }

  public static String postWithJson(String url, String json) {
    DefaultHttpClient httpclient = new DefaultHttpClient();
    HttpPost hp = new HttpPost(url);
    hp.addHeader(HTTP.CONTENT_TYPE, APPLICATION_JSON);

    StringEntity se = null;
    try {
      se = new StringEntity(json, HTTP.UTF_8);
    } catch (UnsupportedCharsetException e) {
      throw new RuntimeException("不支持的编码集", e);
    }

    se.setContentType(CONTENT_TYPE_TEXT_JSON);
    //se.setContentEncoding(new BasicHeader(HTTP.CONTENT_TYPE, APPLICATION_JSON));

    hp.setEntity(se);

    // 发送请求，得到响应
    String responseStr = null;
    try {
      responseStr = (String) httpclient.execute(hp, responseHandler);
    } catch (ClientProtocolException e) {
      throw new RuntimeException("客户端连接协议错误", e);
    } catch (IOException e) {
      throw new RuntimeException("IO操作异常", e);
    } catch (Exception e) {
      throw new RuntimeException("IO操作异常", e);
    } finally {
      abortConnection(hp, httpclient);
    }
    return responseStr;
  }

  public static <T> T postWithJson(String url, Object paramObj, Class<T> valueType) {
    String value = postWithJson(url, ObjectToStr(paramObj));
    return strToObject(value, valueType);
  }

  public static <T> T postWithJson(String url, Object paramObj, TypeReference valueTypeRef) {
    String value = postWithJson(url, ObjectToStr(paramObj));
    return strToObject(value, valueTypeRef);
  }

  public static String putWithJson(String url, String json) {
    DefaultHttpClient httpclient = new DefaultHttpClient();
    HttpPut hp = new HttpPut(url);
    hp.addHeader(HTTP.CONTENT_TYPE, APPLICATION_JSON);

    StringEntity se = null;
    try {
      se = new StringEntity(json, HTTP.UTF_8);
    } catch (UnsupportedCharsetException e) {
      throw new RuntimeException("不支持的编码集", e);
    }

    se.setContentType(CONTENT_TYPE_TEXT_JSON);
    hp.setEntity(se);

    // 发送请求，得到响应
    String responseStr = null;
    try {
      responseStr = (String) httpclient.execute(hp, responseHandler);
    } catch (ClientProtocolException e) {
      throw new RuntimeException("客户端连接协议错误", e);
    } catch (IOException e) {
      throw new RuntimeException("IO操作异常", e);
    } finally {
      abortConnection(hp, httpclient);
    }
    return responseStr;
  }

  public static <T> T putWithJson(String url, Object paramObj, Class<T> valueType) {
    String value = putWithJson(url, ObjectToStr(paramObj));
    return strToObject(value, valueType);
  }

  public static <T> T putWithJson(String url, Object paramObj, TypeReference valueTypeRef) {
    String value = putWithJson(url, ObjectToStr(paramObj));
    return strToObject(value, valueTypeRef);
  }

  public static String delete(String url) {
    return delete(url, new HashMap(), "");
  }

  public static String delete(String url, Map params) {
    return delete(url, params, "");
  }

  public static String delete(String url, Map params, String charset) {
    if (url == null || url.trim().length() == 0) {
      return null;
    }
    List qparams = getParamsList(params);
    if (qparams != null && qparams.size() > 0) {
      charset = (charset == null ? CHARSET_GBK : charset);
      String formatParams = URLEncodedUtils.format(qparams, charset);
      url =
          (url.indexOf("?")) < 0 ? (url + "?" + formatParams)
              : (url.substring(0, url.indexOf("?") + 1) + formatParams);
    }
    DefaultHttpClient httpclient = getDefaultHttpClient(charset);
    HttpDelete hg = new HttpDelete(url);
    // 发送请求，得到响应
    String responseStr = null;
    try {
      responseStr = (String) httpclient.execute(hg, responseHandler);
    } catch (ClientProtocolException e) {
      throw new RuntimeException("客户端连接协议错误", e);
    } catch (IOException e) {
      throw new RuntimeException("IO操作异常", e);
    } finally {
      abortConnection(hg, httpclient);
    }
    return responseStr;
  }

  public static <T> T delete(String url, Class<T> valueType) {
    return delete(url, null, null, valueType);
  }

  public static <T> T delete(String url, Map params, Class<T> valueType) {
    return delete(url, params, null, valueType);
  }

  public static <T> T delete(String url, Map params, String charset, Class<T> valueType) {
    String value = delete(url, params, charset);
    return strToObject(value, valueType);
  }

  public static <T> T delete(String url, TypeReference valueTypeRef) {
    return delete(url, null, null, valueTypeRef);
  }

  public static <T> T delete(String url, Map params, TypeReference valueTypeRef) {
    return delete(url, params, null, valueTypeRef);
  }

  public static <T> T delete(String url, Map params, String charset, TypeReference valueTypeRef) {
    String value = delete(url, params, charset);

    return strToObject(value, valueTypeRef);
  }

  /**
   * 获取DefaultHttpClient实例
   */
  private static DefaultHttpClient getDefaultHttpClient(final String charset) {
    DefaultHttpClient httpclient = new DefaultHttpClient();
    httpclient.getParams().setParameter(CoreProtocolPNames.PROTOCOL_VERSION, HttpVersion.HTTP_1_1);
    // 模拟浏览器，解决一些服务器程序只允许浏览器访问的问题
    httpclient.getParams()
        .setParameter(CoreProtocolPNames.USER_AGENT,
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)");
    httpclient.getParams().setParameter(CoreProtocolPNames.USE_EXPECT_CONTINUE, Boolean.FALSE);
    httpclient.getParams()
        .setParameter(CoreProtocolPNames.HTTP_CONTENT_CHARSET,
            charset == null ? CHARSET_GBK : charset);
    httpclient.setHttpRequestRetryHandler(requestRetryHandler);
    return httpclient;
  }

  /**
   * 释放HttpClient连接
   *
   * @param hrb 请求对象
   * @param httpclient client对象
   */
  private static void abortConnection(final HttpRequestBase hrb, final HttpClient httpclient) {
    if (hrb != null) {
      hrb.abort();
    }
    if (httpclient != null) {
      httpclient.getConnectionManager().shutdown();
    }
  }

  /**
   * 从给定的路径中加载此 KeyStore
   */
  private static KeyStore createKeyStore(final URL url, final String password)
      throws KeyStoreException, NoSuchAlgorithmException, CertificateException,
      IOException {
    if (url == null) {
      throw new IllegalArgumentException("Keystore url may not be null");
    }
    KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
    InputStream is = null;
    try {
      is = url.openStream();
      keystore.load(is, password != null ? password.toCharArray() : null);
    } finally {
      if (is != null) {
        is.close();
        is = null;
      }
    }
    return keystore;
  }

  /**
   * 将传入的键/值对参数转换为NameValuePair参数集
   */
  private static List getParamsList(Map paramsMap) {
    if (paramsMap == null || paramsMap.size() == 0) {
      return null;
    }
    List params = new ArrayList();
    for (Object key : paramsMap.keySet()) {
      if (paramsMap.get(key) != null && !paramsMap.get(key).toString().equals("null")) {
        params.add(new BasicNameValuePair(key.toString(), paramsMap.get(key).toString()));
      }
    }
    return params;
  }

  private static String ObjectToStr(Object object) {
    String result = null;
    if (object != null) {
      try {
        result = mapper.writeValueAsString(object);
      } catch (Exception e) {
        throw new RuntimeException("类型转化异常", e);
      }
    }
    return result;
  }

  private static Map ObjectToMap(Object object) {
    Map result = null;
    if (object != null) {
      try {
        result = mapper.readValue(mapper.writeValueAsString(object), Map.class);
      } catch (Exception e) {
        throw new RuntimeException("类型转化异常", e);
      }
    }
    return result;
  }

  private static <T> T strToObject(String str, Class<T> valueType) {
    T result = null;
    if (str != null) {
      try {
        mapper.configure(Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        result = mapper.readValue(str, valueType);
      } catch (Exception e) {
        throw new RuntimeException("类型转化异常", e);
      }
    }
    return result;
  }

  private static <T> T strToObject(String str, TypeReference valueTypeRef) {
    T result = null;
    if (str != null) {
      try {
        result = mapper.readValue(str, valueTypeRef);
      } catch (Exception e) {
        throw new RuntimeException("类型转化异常", e);
      }
    }
    return result;
  }
}