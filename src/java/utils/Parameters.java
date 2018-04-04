package utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by Administrator on 2016/3/18.
 */
public class Parameters {

  public static final String DataBaseURL;
  public static final String DataBaseUserName;
  public static final String DataBaseUserPassword;
  public static final String JDBCDriverString;
  public static final String MaxConnectionNum;
  public static final String ThreadNum;
  public static final String Home;
  public static final String Dir;
  public static final String ModelDir;



  static {
    Properties properties = new Properties();
    try {
      properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("oracle.properties"));
    } catch (FileNotFoundException ex) {
      System.out.println("读取属性文件--->失败！- 原因：文件路径错误或者文件不存在");
      System.out.println("需要将para.proerties文件所在目录加入到classpath中！");
      ex.printStackTrace();
    } catch (IOException ex) {
      System.out.println("装载文件--->失败!");
      ex.printStackTrace();
    }
    assert (properties.containsKey("DataBaseURL"));
    DataBaseURL = properties.getProperty("DataBaseURL");

    assert (properties.containsKey("MaxConnectionNum"));
    MaxConnectionNum = properties.getProperty("MaxConnectionNum");

    assert (properties.containsKey("DataBaseUserName"));
    DataBaseUserName = properties.getProperty("DataBaseUserName");

    assert (properties.containsKey("ThreadNum"));
    ThreadNum = properties.getProperty("ThreadNum");

    assert (properties.containsKey("DataBaseUserPassword"));
    DataBaseUserPassword = properties.getProperty("DataBaseUserPassword");

    assert (properties.containsKey("JDBCDriverString"));
    JDBCDriverString = properties.getProperty("JDBCDriverString");

    assert (properties.containsKey("Home"));
    Home = properties.getProperty("Home");

    assert (properties.containsKey("Dir"));
    Dir = properties.getProperty("Dir");

    assert (properties.containsKey("ModelDir"));
    ModelDir = properties.getProperty("ModelDir");
  }

}
