import org.geotools.data.DataStore;
import org.geotools.data.FeatureSource;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.*;

/**
 * @Classname main
 * @Description
 * @Date 2021/9/9 13:08
 * @Created by xiaocai
 */
public class main {

    public static void main(String[] args) throws Exception {

        String strFilePath = "C:\\Users\\HTHT\\Desktop\\文档说明\\田长制\\数据\\shp\\project\\0901\\xzqinfo12_200_Project.shp";
        ShapefileDataStore featureClassSource = ConnPostgis.connectShapefile(strFilePath);

        String strHost="10.1.100.153";
        String port="5432";
        String strDataBaseName="clp_ys";
        String strSchemaName="clp";
        String strUserName="clp";
        String strPwd="clp";

        String strPgUrl=String.format("jdbc:postgresql://%s:%s/%s",strHost,port,strDataBaseName);

        String strTableName="xzqinfo12_200";
        String inputDataBaseItemName=String.format("%s%s",strTableName,"_20220901");
        inputDataBaseItemName=inputDataBaseItemName.toLowerCase();

        DataStore pgDatastore = ConnPostgis.getPostgisDataStore("postgis", strHost, port, strDataBaseName, strSchemaName, strUserName,strPwd);

//        geotoolsInsertData(featureClassSource,pgDatastore,inputDataBaseItemName);

        sqlInsertData(featureClassSource,pgDatastore,inputDataBaseItemName,strPgUrl,strUserName,strPwd);
    }

    private static void sqlInsertData(ShapefileDataStore featureClassSource, DataStore pgDatastore, String inputDataBaseItemName,String strPgUrl,String strUserName,String strPwd ) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {

            List<AttributeDescriptor> attrList = featureClassSource.getSchema().getAttributeDescriptors();
            SimpleFeatureStore pgFeatureDatastore = null;
            try
            {
                pgFeatureDatastore = (SimpleFeatureStore) pgDatastore.getFeatureSource(inputDataBaseItemName.toLowerCase());
            }
            catch(Exception exx)
            {
                pgFeatureDatastore = null;
            }
            if(pgFeatureDatastore==null) {
                FeatureWriter<SimpleFeatureType, SimpleFeature> writer = createFeaClass(featureClassSource, pgDatastore, attrList, inputDataBaseItemName.toLowerCase());
                pgFeatureDatastore = (SimpleFeatureStore) pgDatastore.getFeatureSource(inputDataBaseItemName.toLowerCase());
            }
            if (pgFeatureDatastore != null) {
                connection = DbConnectionUtil.getConnection(strPgUrl, strUserName,strPwd);
                FeatureSource<SimpleFeatureType, SimpleFeature> featureSource = null;
                //getTypeNames:获取所有地理图层
                String typeName = featureClassSource.getTypeNames()[0];
                featureSource = (FeatureSource<SimpleFeatureType, SimpleFeature>) featureClassSource.getFeatureSource(typeName);
                //一个用于处理FeatureCollection的实用工具类。提供一个获取FeatureCollection实例的机制
                FeatureCollection<SimpleFeatureType, SimpleFeature> result = featureSource.getFeatures();
                FeatureIterator<SimpleFeature> itertor = result.features();

                Map<String, Object> mapValues = new LinkedHashMap<>();
                List<AttributeDescriptor> pgAttrList = pgFeatureDatastore.getSchema().getAttributeDescriptors();
                String strInsertSql = getPgClassInsertSql(inputDataBaseItemName.toLowerCase(), pgAttrList, mapValues);
                preparedStatement = connection.prepareStatement(strInsertSql);
                Integer index = 1;
                while (itertor.hasNext()) {
                    SimpleFeature feature = itertor.next();
                    Collection<Property> p = feature.getProperties();
                    Iterator<Property> it = p.iterator();
                    while (it.hasNext()) {
                        Property pro = it.next();
                        String strFieldName = pro.getName().toString().toLowerCase();
                        if (mapValues.containsKey(strFieldName)) {
                            mapValues.put(strFieldName, pro.getValue());
                        }
                    }

                    Integer MapIndex = 1;
                    for (String key : mapValues.keySet()) {
                        if (key.toString().equals(ConstField.THEGEOM)) {
                            preparedStatement.setString(MapIndex++, mapValues.get(key).toString());
                        } else {
                            Object objVal=mapValues.get(key);
                            if(objVal instanceof Date)
                            {
                                // 1) 创建java.util.Date的对象

                                java.util.Date utilDate = (Date)objVal;

                                long datems  = utilDate.getTime();

                                java.sql.Date  sqlDate = new java.sql.Date(datems);

                                preparedStatement.setDate(MapIndex++, sqlDate);
                            }
                            else
                            {
                                preparedStatement.setObject(MapIndex++, objVal);
                            }
                        }
                    }
                    preparedStatement.addBatch();
                    index++;
                    if (index % 5000 == 0) {
                        System.out.println(index.toString());
                        preparedStatement.executeBatch();
                        preparedStatement.clearBatch();
                    }
                }
                System.out.println(index.toString());
                itertor.close();
                preparedStatement.executeBatch();

                pgDatastore.dispose();//使用之后必须关掉
                featureClassSource.dispose();//使用之后必须关掉
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static String getPgClassInsertSql(String strFeaClassName, List<AttributeDescriptor> pgAttrList, Map<String,Object> mapValues) {
        List<String> listValues = new ArrayList<>();
        List<String> listFields = new ArrayList<>();

        for (int i = 0; i < pgAttrList.size(); i++) {
            AttributeDescriptor attributeDescriptor = pgAttrList.get(i);

            if (attributeDescriptor.getName().toString().equals(ConstField.FID)) {
                continue;
            }
            listFields.add(attributeDescriptor.getName().toString());
            if (attributeDescriptor.getName().toString().equals(ConstField.THEGEOM)) {
                listValues.add("st_geomfromText(?)");
            } else {
                listValues.add("?");
            }
            mapValues.put(attributeDescriptor.getName().toString(), null);
        }
        String strInsertSql = String.format("insert into %s(%s) values(%s)", strFeaClassName, String.join(",", listFields), String.join(",", listValues));
        return strInsertSql;
    }

    private static void geotoolsInsertData(ShapefileDataStore featureClassSource, DataStore pgDatastore,String inputDataBaseItemName) {

        try {
            if (pgDatastore != null) {
                List<AttributeDescriptor> attrList = featureClassSource.getSchema().getAttributeDescriptors();
                FeatureWriter<SimpleFeatureType, SimpleFeature> writer = createFeaClass(featureClassSource, pgDatastore, attrList, inputDataBaseItemName);
                if (writer != null) {
                    FeatureSource<SimpleFeatureType, SimpleFeature> featureSource = null;
                    //getTypeNames:获取所有地理图层
                    String typeName = featureClassSource.getTypeNames()[0];
                    featureSource = (FeatureSource<SimpleFeatureType, SimpleFeature>) featureClassSource.getFeatureSource(typeName);
                    //一个用于处理FeatureCollection的实用工具类。提供一个获取FeatureCollection实例的机制
                    FeatureCollection<SimpleFeatureType, SimpleFeature> result = featureSource.getFeatures();
                    FeatureIterator<SimpleFeature> itertor = result.features();
                    SimpleFeature writeFeature = writer.next();
                    Integer index = 0;
                    while (itertor.hasNext()) {
                        SimpleFeature feature = itertor.next();
                        Collection<Property> p = feature.getProperties();
                        Iterator<Property> it = p.iterator();
                        while (it.hasNext()) {
                            Property pro = it.next();
                            if (pro.getName().toString().equals("geom")) {
                                writeFeature.setAttribute("the_geom", pro.getValue());
                            } else {
                                writeFeature.setAttribute(pro.getName().toString().toLowerCase(), pro.getValue());
                            }
                        }
                        if (index % 10000 == 0) {
                            System.out.println(index);
                        }
                        //写入
                        writer.write();
                        //再来一个点
                        writeFeature = writer.next();
                        index++;
                    }
                    System.out.println(index);
                    itertor.close();
                    //关闭
                    writer.close();
                    pgDatastore.dispose();//使用之后必须关掉
                    featureClassSource.dispose();//使用之后必须关掉
                }
            }
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
    }


    /**
     * 通过Shapefile文件创建要素类
     * @param shapefileDataStore
     * @param dataStore
     * @param attrList
     * @param strFeaClassName
     * @return
     */
    public static FeatureWriter<SimpleFeatureType, SimpleFeature> createFeaClass(ShapefileDataStore shapefileDataStore,DataStore dataStore,List<AttributeDescriptor> attrList,String strFeaClassName) {

        try {
            //SimpleFeatureTypeBuilder 构造简单特性类型的构造器
            SimpleFeatureTypeBuilder tBuilder = new SimpleFeatureTypeBuilder();
            tBuilder.setName(strFeaClassName);

            SimpleFeatureType schema = shapefileDataStore.getSchema();
            CoordinateReferenceSystem dataCrs = schema.getCoordinateReferenceSystem();
            tBuilder.setCRS(dataCrs);

            for (int i = 0; i < attrList.size(); i++) {
                AttributeDescriptor attributeDescriptor = attrList.get(i);
                Class<?> classType= (Class.forName(attributeDescriptor.getType().getBinding().getName()));
                if(java.lang.Double.class.isAssignableFrom(classType)||java.lang.Float.class.isAssignableFrom(classType))
                {
                    tBuilder.add(attributeDescriptor.getName().toString().toLowerCase(), BigDecimal.class);
                }
                else
                {
                    tBuilder.add(attributeDescriptor.getName().toString().toLowerCase(), classType);
                }
            }

            //设置此数据存储的特征类型
            dataStore.createSchema(tBuilder.buildFeatureType());
            FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dataStore.getFeatureWriterAppend(strFeaClassName, Transaction.AUTO_COMMIT);
            return writer;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
