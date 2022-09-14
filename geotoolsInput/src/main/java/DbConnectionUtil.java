import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.Properties;
import java.util.Set;

/**
 * @BelongsProject: geotoolsInput
 * @BelongsPackage: PACKAGE_NAME
 * @Author: 蔡名洋
 * @CreateTime: 2022-08-23  10:01
 * @Description: TODO
 * @Version: 1.0
 */
public class DbConnectionUtil {
    private static final String URL="spring.datasource.dynamic.datasource.task.url";
    private static final String USERNAME="spring.datasource.dynamic.datasource.task.username";
    private static final String PASSWORD="spring.datasource.dynamic.datasource.task.password";

    private String adminType = "";
    private Connection CurrentDataBaseConnection = null;

    /**
     * 获取数据库连接
     * @param url   数据库的URL
     * @param userName 数据库的用户名
     * @param passWord  数据库的密码
     * @return
     */
    public static Connection getConnection(String url, String userName, String passWord) {
        Connection connection = null;
        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection(url, userName, passWord);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 关闭数据库连接
     * @param connection
     */
    public static void closeDatabase(Connection connection) {
        try {
            if (connection !=null){
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param statement
     */
    public static void closeDatabase(Statement statement){
        try {
            if (statement!= null){
                statement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭数据库连接
     * @param connection
     * @param statement
     */
    public static void closeDatabase(Connection connection, Statement statement) {
        try {
            if (connection !=null){
                connection.close();
            }
            if (statement!= null){
                statement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     *  获取sql执行结果
     * @param connection
     * @param strSql
     * @return
     * @throws Exception
     */
    public static double getSqlResultValue(Connection connection,String strSql) throws Exception {
        Statement statement =null;
        ResultSet resultSet=null;
        try
        {
            statement = connection.createStatement();
            resultSet=statement.executeQuery(strSql);
            if(resultSet!=null) {
                resultSet.next();
                Double result = resultSet.getDouble(1);
                return result;
            }
        }
        catch (Exception exception)
        {
            //若sql执行失败，则抛出异常
            throw new Exception(exception.getMessage());
        }
        finally {
            closeDatabase(statement,resultSet);
        }
        return 0;
    }

    /**
     * 关闭数据库连接
     * @param connection
     * @param statement
     * @param resultSet
     */
    public static void closeDatabase(Connection connection, Statement statement, ResultSet resultSet) {
        try {
            if (connection !=null){
                connection.close();
            }
            if (statement!= null){
                statement.close();
            }
            if (resultSet!=null){
                resultSet.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void closeDatabase(Statement statement, ResultSet resultSet){
        try {
            if (statement!= null){
                statement.close();
            }
            if (resultSet!=null){
                resultSet.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 数据库连接验证
     * @param properties
     * @param sets
     * @return
     */
    public static String validateDatasourceConnection(Properties properties, Set<Object> sets){
        String validateMessage = "";
        Connection connection = null;
        try {
            String url = "";
            String userName = "";
            String passWord = "";
            for (Object object : sets) {
                if (object.toString().trim().equals(URL)){
                    url = new String(properties.getProperty((String) object).getBytes());
                }
                if (object.toString().trim().equals(USERNAME)){
                    userName = new String(properties.getProperty((String) object).getBytes());
                }
                if (object.toString().trim().equals(PASSWORD)){
                    passWord = new String(properties.getProperty((String) object).getBytes());
                }
            }
            if (StringUtils.isNotBlank(url)){
                if (StringUtils.isNotBlank(userName) && StringUtils.isNotBlank(passWord)){
                    connection = DriverManager.getConnection(url, userName, passWord);
                    if (connection == null){
                        validateMessage = "请检查数据库IP地址、端口号是否正确!";
                    }
                }else{
                    validateMessage = "用户名、密码不能为空!";
                }
            }else{
                validateMessage = "数据库连接地址不能为空!";
            }
        }catch (Exception ex){
            validateMessage = ex.getMessage();
        }finally {
            if (connection !=null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        return  validateMessage;
    }

    /**
     * 执行sql语句
     * @param connection
     * @param strSql
     */
    public static void executeSql(Connection connection, String strSql) {
        Statement statement =null;
        try {
            statement = connection.createStatement();
            statement.execute(strSql);
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
        }
        finally {
            closeDatabase(statement);
        }
    }
}

