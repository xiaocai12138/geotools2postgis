import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.data.postgis.PostgisNGDataStoreFactory;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * @Classname ConnPostgis
 * @Description
 * @Date 2021/9/9 13:10
 * @Created by xiaocai
 */
public class ConnPostgis {
    /**
     * @param dbtype: 数据库类型，postgis or mysql
     * @param host: ip地址
     * @param port: 端口号
     * @param database: 需要连接的数据库
     * @param userName: 用户名
     * @param password: 密码
     * @return: 返回为FeatureCollection类型
     */
    public static  DataStore getPostgisDataStore(String dbtype, String host, String port,
                                                 String database,String schema, String userName, String password) {
        Map<String, Object> params = new HashMap<String, Object>(16);
        DataStore pgDatastore=null;
        //需要连接何种数据库，postgis or mysql
        params.put(PostgisNGDataStoreFactory.DBTYPE.key, dbtype);
        //ip地址
        params.put(PostgisNGDataStoreFactory.HOST.key, host);
        //端口号
        params.put(PostgisNGDataStoreFactory.PORT.key, new Integer(port));
        //需要连接的数据库
        params.put(PostgisNGDataStoreFactory.DATABASE.key, database);
        params.put(PostgisNGDataStoreFactory.SCHEMA.key, schema);
        params.put(PostgisNGDataStoreFactory.USER.key, userName);
        params.put(PostgisNGDataStoreFactory.PASSWD.key, password);
        try {
            //获取存储空间
            pgDatastore = DataStoreFinder.getDataStore(params);

            if (pgDatastore != null) {
                System.out.println("系统连接到位于：" + host + "的空间数据库" + database
                        + "成功！");
            } else {
                System.out.println("系统连接到位于：" + host + "的空间数据库" + database
                        + "失败！请检查相关参数");

            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("系统连接到位于：" + host + "的空间数据库" + database
                    + "失败！请检查相关参数");
        }
        return pgDatastore;
    }


    /**
     * 连接Shapefile文件
     * @param strFilePath
     * @return
     */
    public static ShapefileDataStore connectShapefile(String strFilePath) {
        try {
            //一个数据存储实现，允许从Shapefiles读取和写入
            ShapefileDataStore shpDataStore = null;
            shpDataStore = new ShapefileDataStore(new File(strFilePath).toURI().toURL());
            shpDataStore.setCharset(Charset.forName("UTF-8"));
            //获取这个数据存储保存的类型名称数组
            //getTypeNames:获取所有地理图层
            String typeName = shpDataStore.getTypeNames()[0];
            //通过此接口可以引用单个shapefile、数据库表等。与数据存储进行比较和约束
            FeatureSource<SimpleFeatureType, SimpleFeature> featureSource = null;
            featureSource = (FeatureSource<SimpleFeatureType, SimpleFeature>) shpDataStore.getFeatureSource(typeName);

            return shpDataStore;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
