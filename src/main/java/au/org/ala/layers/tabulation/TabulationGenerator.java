/**************************************************************************
 *  Copyright (C) 2010 Atlas of Living Australia
 *  All Rights Reserved.
 *
 *  The contents of this file are subject to the Mozilla Public
 *  License Version 1.1 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://www.mozilla.org/MPL/
 *
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 ***************************************************************************/
package au.org.ala.layers.tabulation;

import au.org.ala.layers.client.Client;
import au.org.ala.layers.dao.FieldDAO;
import au.org.ala.layers.dao.LayerDAO;
import au.org.ala.layers.dao.LayerIntersectDAO;
import au.org.ala.layers.dao.ObjectDAO;
import au.org.ala.layers.dto.*;
import au.org.ala.layers.dto.Objects;
import au.org.ala.layers.intersect.Grid;
import au.org.ala.layers.intersect.SimpleRegion;
import au.org.ala.layers.intersect.SimpleShapeFile;
import au.org.ala.layers.util.SpatialUtil;
import au.org.ala.spatial.analysis.layers.Records;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.log4j.Logger;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * @author Adam
 */
public class TabulationGenerator {

    private static final Logger LOGGER = Logger.getLogger(TabulationGenerator.class);
    static int CONCURRENT_THREADS = 6;
    static String db_url = "jdbc:postgresql://localhost:5432/layersdb";
    static String db_usr = "postgres";
    static String db_pwd = "postgres";
    static String allFidPairsSQL = "SELECT "
            + "(CASE WHEN f1.id < f2.id THEN f1.id ELSE f2.id END) as fid1, "
            + "(CASE WHEN f1.id < f2.id THEN f2.id ELSE f1.id END) as fid2, "
            + "(CASE WHEN f1.id < f2.id THEN f1.domain ELSE f2.domain END) as domain1, "
            + "(CASE WHEN f1.id < f2.id THEN f2.domain ELSE f1.domain END) as domain2 "
            + "FROM "
            + "(select f3.id, l1.domain from fields f3, layers l1 where f3.intersect=true AND f3.spid='' || l1.id) f1, "
            + "(select f4.id, l2.domain from fields f4, layers l2 where f4.intersect=true AND f4.spid='' || l2.id) f2 "
            + "WHERE f1.id != f2.id "
            + "group by fid1, fid2, domain1, domain2 "
            + "order by fid1, fid2";
    static String existingTabulationssql = "SELECT fid1, fid2 from tabulation group by fid1, fid2";
    public static String fidPairsToProcessSQL = "SELECT a.fid1, a.domain1, a.fid2, a.domain2 FROM ("
            + allFidPairsSQL
            + ") a WHERE (a.fid1, a.fid2) NOT IN ("
            + existingTabulationssql
            + ") group by a.fid1, a.fid2, a.domain1, a.domain2;";
    static String runningTabulations = "SELECT * FROM pg_catalog.pg_stat_activity WHERE query like '%group by fid1, fid2, domain1, domain2%'";
    private static Records recordsOne = null;
    private static Connection connection;

    public static Connection getConnection() {
        boolean closed = false;
        try {
            if (connection != null) {
                closed = connection.isClosed();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (connection == null || closed) {
            Connection conn = null;
            try {
                Class.forName("org.postgresql.Driver");
                String url = db_url;
                conn = DriverManager.getConnection(url, db_usr, db_pwd);

            } catch (Exception e) {
                System.out.println("Unable to create Connection");
                e.printStackTrace(System.out);
            }
            connection = conn;
        }

        return connection;
    }

    static public void main(String[] args) throws IOException {
        System.out.println("args[0] = threadcount," + "\nargs[1] = db connection string," +
                "\n args[2] = db username," + "\n args[3] = password,"
                + "\n args[4] = (optional) specify one step to run, " +
                "'1' pair objects, '3' delete invalid objects, '4' area, '5' occurrences, '6' grid x grid comparisons"
                + "\n args[5] = (required when args[4]=5 or 6) path to records file,");

        if (args.length >= 5) {
            CONCURRENT_THREADS = Integer.parseInt(args[0]);
            db_url = args[1];
            db_usr = args[2];
            db_pwd = args[3];
        }

        if (args.length < 5) {
            System.out.println("all");

            updatePairObjects();

            deleteInvalidObjects();

            long start = System.currentTimeMillis();
            while (updateArea() > 0) {
                System.out.println("time since start= " + (System.currentTimeMillis() - start) + "ms");
            }
        } else if (args[4].equals("1")) {
            System.out.println("1");
            updatePairObjects();
        } else if (args[4].equals("2")) {
            System.out.println("2");
            // updateSingleObjects();
        } else if (args[4].equals("3")) {
            System.out.println("3");
            deleteInvalidObjects();
        } else if (args[4].equals("4")) {
            System.out.println("4");
            long start = System.currentTimeMillis();
            while (updateArea() > 0) {
                System.out.println("time since start= " + (System.currentTimeMillis() - start) + "ms");
            }
        } else if (args[4].equals("5")) {
            System.out.println("5");
            // some init
            FieldDAO fieldDao = Client.getFieldDao();
            LayerDAO layerDao = Client.getLayerDao();
            ObjectDAO objectDao = Client.getObjectDao();
            LayerIntersectDAO layerIntersectDao = Client.getLayerIntersectDao();

            // test fieldDao
            System.out.println("TEST: " + fieldDao.getFields());
            System.out.println("RECORDS FILE: " + args[5]);

            File f = new File(args[5]);
            if (f.exists()) {
                Records records = new Records(f.getAbsolutePath());
                // while (updateOccurrencesSpecies(records) > 0) {
                // System.out.println("time since start= " +
                // (System.currentTimeMillis() - start) + "ms");
                // }
                updateOccurrencesSpecies2(records, CONCURRENT_THREADS, null);
            } else {
                System.out.println("Please provide a valid path to the species occurrence file");
            }
        } else if (args[4].equals("6")) {
            System.out.println("6");

            // some init
            FieldDAO fieldDao = Client.getFieldDao();
            LayerDAO layerDao = Client.getLayerDao();
            ObjectDAO objectDao = Client.getObjectDao();
            LayerIntersectDAO layerIntersectDao = Client.getLayerIntersectDao();

            // test fieldDao
            System.out.println("TEST: " + fieldDao.getFields());
            System.out.println("RECORDS FILE: " + args[5]);

            File f = new File(args[5]);
            if (f.exists()) {
                Records records = new Records(f.getAbsolutePath());
                updatePairObjectsGridToGrid(records);
            } else {
                System.out.println("Please provide a valid path to the species occurrence file");
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void all(String recordsFilePath, Connection connection) {
        TabulationGenerator.connection = connection;

        waitForRunningTabulationsToFinish();

        updatePairObjects();

        deleteInvalidObjects();

        while (updateArea() > 0) ;

        File f = new File(recordsFilePath);
        //not going to reopen the records file
        if (f.exists() && recordsOne == null) {
            try {
                recordsOne = new Records(f.getAbsolutePath());
            } catch (IOException e) {
                LOGGER.error("failed to open records file: " + recordsFilePath, e);
            }

            updateOccurrencesSpecies2(recordsOne, CONCURRENT_THREADS, null);

            updatePairObjectsGridToGrid(recordsOne);
        } else {
            System.out.println("Please provide a valid path to the species occurrence file");
        }
    }

    private static void waitForRunningTabulationsToFinish() {
        Connection conn = null;
        try {
            ResultSet rs1 = null;
            conn = getConnection();
            String sql = runningTabulations;
            Statement s1 = conn.createStatement();

            while (rs1 == null || rs1.getFetchSize() > 0) {
                if (rs1 != null) {
                    Thread.sleep(5 * 60000);
                }
                rs1 = s1.executeQuery(sql);
            }
        } catch (Exception e) {
            LOGGER.error("error waiting for running tabulations to finish");
        }
    }

    private static void updatePairObjects() {
        Connection conn = null;
        try {
            conn = getConnection();
            String sql = fidPairsToProcessSQL;

            Statement s2 = conn.createStatement();
            ResultSet rs2 = s2.executeQuery("select * from layers");
            Map layerMap = new HashMap();
            while (rs2.next()) {
                layerMap.put(rs2.getString("id"), rs2.getString("path_orig"));
            }
            rs2.close();
            s2.close();

            Statement s3 = conn.createStatement();
            ResultSet rs3 = s3.executeQuery("select * from fields");
            Map fieldSpid = new HashMap();
            while (rs3.next()) {
                fieldSpid.put(rs3.getString("id"), rs3.getString("spid"));
            }
            rs3.close();
            s3.close();

            Statement s1 = conn.createStatement();
            ResultSet rs1 = s1.executeQuery(sql);
            ConcurrentLinkedQueue<String> data = new ConcurrentLinkedQueue<String>();
            while (rs1.next()) {
                // check file sizes
                String layer1 = (String) fieldSpid.get(rs1.getString("fid1"));
                String layer2 = (String) fieldSpid.get(rs1.getString("fid2"));
                String path1 = (String) layerMap.get(layer1);
                String path2 = (String) layerMap.get(layer2);
                File f1 = new File(Client.getLayerIntersectDao().getConfig().getLayerFilesPath() + path1 + ".shp");
                File f2 = new File(Client.getLayerIntersectDao().getConfig().getLayerFilesPath() + path2 + ".shp");

                //domain test
                if (isSameDomain(parseDomain(rs1.getString("domain1")), parseDomain(rs1.getString("domain2")))) {
                    //if (f1.exists() && f2.exists() && f1.length() < 50 * 1024 * 1024 && f2.length() < 50 * 1024 * 1024) {
                    System.out.println("will do tabulation on: " + rs1.getString("fid1") + ", " + rs1.getString("fid2"));
                    data.add(rs1.getString("fid1") + "," + rs1.getString("fid2"));
                    // } else {
                    //for gridToGrid
                    // }
                }
            }
            rs1.close();
            s1.close();

            System.out.println("next " + data.size());

            int size = data.size();

            if (size == 0) {
                return;
            }

            DistributionThread[] threads = new DistributionThread[CONCURRENT_THREADS];
            for (int j = 0; j < CONCURRENT_THREADS; j++) {
                connection = null;
                threads[j] = new DistributionThread(data);
                threads[j].start();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static String[] parseDomain(String domain) {
        if (domain == null || domain.length() == 0) {
            return null;
        }
        String[] domains = domain.split(",");
        for (int i = 0; i < domains.length; i++) {
            domains[i] = domains[i].trim();
        }
        return domains;
    }

    public static boolean isSameDomain(String[] domain1, String[] domain2) {
        if (domain1 == null || domain2 == null) {
            return true;
        }

        for (String s1 : domain1) {
            for (String s2 : domain2) {
                if (s1.equalsIgnoreCase(s2)) {
                    return true;
                }
            }
        }

        return false;
    }

    private static void updatePairObjectsGridToGrid(Records records) {
        Connection conn = null;
        try {
            conn = getConnection();
            String sql = fidPairsToProcessSQL;
            Statement s1 = conn.createStatement();
            Statement s2 = conn.createStatement();
            ResultSet rs1 = s1.executeQuery(sql);

            while (rs1.next()) {
                // check file sizes
                String layer1 = Client.getFieldDao().getFieldById(rs1.getString("fid1")).getSpid();
                String layer2 = Client.getFieldDao().getFieldById(rs1.getString("fid2")).getSpid();
                String path1 = Client.getLayerDao().getLayerById(Integer.parseInt(layer1)).getPath_orig();
                String path2 = Client.getLayerDao().getLayerById(Integer.parseInt(layer2)).getPath_orig();
                File f1 = new File(Client.getLayerIntersectDao().getConfig().getLayerFilesPath() + path1 + ".shp");
                File f2 = new File(Client.getLayerIntersectDao().getConfig().getLayerFilesPath() + path2 + ".shp");

                // domain test
                if (isSameDomain(parseDomain(rs1.getString("domain1")), parseDomain(rs1.getString("domain2")))) {
                    if (f1.exists() && f2.exists() && f1.length() < 50 * 1024 * 1024 && f2.length() < 50 * 1024 * 1024) {
                        // for shape comparisons
                    } else {
                        System.out.println("gridToGrid: " + rs1.getString("fid1") + ", " + rs1.getString("fid2"));
                        // for gridToGrid
                        sql = gridToGrid(rs1.getString("fid1"), rs1.getString("fid2"), records);
                        s2.execute(sql);
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static String gridToGrid(String fieldId1, String fieldId2, Records records) throws IOException {
        FileWriter fw = new FileWriter(fieldId1 + "_" + fieldId2 + ".sql");

        List<Double> resolutions = Client.getLayerIntersectDao().getConfig().getAnalysisResolutions();
        Double resolution = resolutions.get(0);

        // check if resolution needs changing
        resolution = Double.parseDouble(confirmResolution(new String[]{fieldId1, fieldId2}, String.valueOf(resolution)));
        System.out.println("RESOLUTION: " + resolution);

        // get extents for all layers
        double[][] field1Extents = getLayerExtents(String.valueOf(resolution), fieldId1);
        System.out.println("Extents for " + fieldId1 + ": " + field1Extents);

        double[][] field2Extents = getLayerExtents(String.valueOf(resolution), fieldId2);
        System.out.println("Extents for " + fieldId2 + ": " + field2Extents);

        double[][] extents = internalExtents(field1Extents, field2Extents);
        System.out.println("Internal extents: " + extents);
        if (!isValidExtents(extents)) {
            System.out.println("Warning, no overlap between grids: " + fieldId1 + " and " + fieldId2);
            return null;
        }

        // get mask and adjust extents for filter
        int width = 0, height = 0;
        System.out.println("resolution: " + resolution);
        height = (int) Math.ceil((extents[1][1] - extents[0][1]) / resolution);
        width = (int) Math.ceil((extents[1][0] - extents[0][0]) / resolution);

        // prep grid files
        String pth1 = getLayerPath("" + resolution, fieldId1);
        String pth2 = getLayerPath("" + resolution, fieldId2);
        System.out.println("PATH 1: " + pth1);
        System.out.println("PATH 2: " + pth2);
        Grid grid1 = new Grid(pth1);
        Grid grid2 = new Grid(pth2);
        Properties p1 = new Properties();
        p1.load(new FileReader(pth1 + ".txt"));
        Properties p2 = new Properties();
        p2.load(new FileReader(pth2 + ".txt"));

        // pids
        List<Objects> objects1 = Client.getObjectDao().getObjectsById(fieldId1);
        List<Objects> objects2 = Client.getObjectDao().getObjectsById(fieldId2);

        // get pids for properties entries
        for (Entry<Object, Object> entry : p1.entrySet()) {
            for (Objects o : objects1) {
                if ((o.getId() == null && entry.getValue() == null) || (o.getId() != null && entry.getValue() != null && o.getId().equalsIgnoreCase(((String) entry.getValue())))) {
                    entry.setValue(o.getPid());
                    break;
                }
            }
        }
        for (Entry<Object, Object> entry : p2.entrySet()) {
            for (Objects o : objects2) {
                if ((o.getId() == null && entry.getValue() == null) || (o.getId() != null && entry.getValue() != null && o.getId().equalsIgnoreCase(((String) entry.getValue())))) {
                    entry.setValue(o.getPid());
                    break;
                }
            }
        }

        HashMap<String, Pair> map = new HashMap<String, Pair>();

        // sample on species
        if (records != null) {
            for (int i = 0; i < records.getRecordsSize(); i++) {
                // get v1 & v2
                int v1 = (int) grid1.getValues3(new double[][]{{records.getLongitude(i), records.getLatitude(i)}}, 1024 * 1024)[0];
                int v2 = (int) grid2.getValues3(new double[][]{{records.getLongitude(i), records.getLatitude(i)}}, 1024 * 1024)[0];
                String key = v1 + " " + v2;
                Pair p = map.get(key);
                if (p == null) {
                    p = new Pair(key);
                    map.put(key, p);
                }
                p.species.set(records.getSpeciesNumber(i));
                p.occurrences++;
            }
        }

        // build intersections by category pairs
        for (int i = 0; i < width; i++) {
            for (int j = 0; j < height; j++) {
                // area
                int v1 = (int) grid1.getValues3(new double[][]{{extents[0][0] + resolution * i, extents[0][1] + resolution * j}}, 40960)[0];
                int v2 = (int) grid2.getValues3(new double[][]{{extents[0][0] + resolution * i, extents[0][1] + resolution * j}}, 40960)[0];
                String key = v1 + " " + v2;
                Pair p = map.get(key);
                if (p == null) {
                    p = new Pair(key);
                    map.put(key, p);
                }
                p.area += SpatialUtil.cellArea(resolution, extents[0][1] + resolution * j) * 1000000; // convert
                // sqkm
                // to
                // sqm
            }
        }

        // sql statements to put pairs into tabulation
        StringBuilder sb = new StringBuilder();
        for (Entry<String, Pair> p : map.entrySet()) {
            if (p1.get(p.getValue().v1) != null && p2.get(p.getValue().v2) != null) {
                String sql = "INSERT INTO tabulation (fid1, fid2, pid1, pid2, area, occurrences, species) VALUES " + "('" + fieldId1 + "','" + fieldId2 + "'," + "'" + p1.get(p.getValue().v1) + "','"
                        + p2.get(p.getValue().v2) + "'," + p.getValue().area + "," + p.getValue().occurrences + "," + p.getValue().species.cardinality() + ");";

                sb.append(sql);

                fw.write(sql);
                fw.write("\n");
                fw.flush();
            }
        }

        fw.close();

        return sb.toString();
    }

    public static void shpIntersection(String fieldId1, String shapeFile1, String idColumn1,
                                       String fieldId2, String shapeFile2, String idColumn2,
                                       File shpIntersectionFile, Records records, SimpleJdbcTemplate jdbcTemplate) throws IOException, ParseException {
        //open both shapefiles and build (shpId => objectId) map.
        SimpleShapeFile shp1 = new SimpleShapeFile(shapeFile1, idColumn1);
        SimpleShapeFile shp2 = new SimpleShapeFile(shapeFile2, idColumn2);

        Map<String, String> p1 = new HashMap<String, String>();
        Map<String, String> p2 = new HashMap<String, String>();

        Map map1 = new HashMap();
        map1.put("url", new File(shapeFile1).toURI().toURL());
        DataStore dataStore1 = DataStoreFinder.getDataStore(map1);
        String typeName1 = dataStore1.getTypeNames()[0];
        FeatureSource source = dataStore1.getFeatureSource(typeName1);
        FeatureIterator iterator1 = source.getFeatures().features();
        while (iterator1.hasNext()) {
            SimpleFeature feature1 = (SimpleFeature) iterator1.next();
            p1.put(feature1.getID(), feature1.getAttribute(idColumn1).toString());
        }

        map1 = new HashMap();
        map1.put("url", new File(shapeFile1).toURI().toURL());
        dataStore1 = DataStoreFinder.getDataStore(map1);
        typeName1 = dataStore1.getTypeNames()[0];
        source = dataStore1.getFeatureSource(typeName1);
        iterator1 = source.getFeatures().features();
        while (iterator1.hasNext()) {
            SimpleFeature feature1 = (SimpleFeature) iterator1.next();
            p2.put(feature1.getID(), feature1.getAttribute(idColumn1).toString());
        }

        HashMap<String, Pair> map = new HashMap<String, Pair>();

        // sample on species
        if (records != null) {
            for (int i = 0; i < records.getRecordsSize(); i++) {
                // get v1 & v2
                String v1 = shp1.intersect(records.getLongitude(i), records.getLatitude(i));
                String v2 = shp2.intersect(records.getLongitude(i), records.getLatitude(i));
                String key = v1 + " " + v2;
                Pair p = map.get(key);
                if (p == null) {
                    p = new Pair(key);
                    map.put(key, p);
                }
                p.species.set(records.getSpeciesNumber(i));
                p.occurrences++;
            }
        }

        //iterate over shpIntersectionFile
        ZipInputStream zis = new ZipInputStream(new FileInputStream(shpIntersectionFile));
        ZipEntry ze = zis.getNextEntry();
        InputStreamReader isr = new InputStreamReader(zis);
        BufferedReader br = new BufferedReader(isr);
        String id1;
        String id2;
        String wkt;
        while ((id1 = br.readLine()) != null) {
            id2 = br.readLine();
            wkt = br.readLine();

            String key = p1.get(id1) + " " + p2.get(id2);
            Pair p = map.get(key);
            if (p == null) {
                p = new Pair(key);
                map.put(key, p);
            }
            p.area += SpatialUtil.calculateArea(wkt);

            WKTReader wktReader = new WKTReader();
            Geometry geom = wktReader.read(wkt);
            if (p.geom == null) {
                p.geom = geom;
            } else {
                p.geom = p.geom.union(geom);
            }
        }

        //map (objectId => pid)
        List<Objects> objects1 = Client.getObjectDao().getObjectsById(fieldId1);
        List<Objects> objects2 = Client.getObjectDao().getObjectsById(fieldId2);
        Map<String, String> pids1 = new HashMap<String, String>();
        Map<String, String> pids2 = new HashMap<String, String>();
        for (Objects o : objects1) {
            pids1.put(o.getId(), o.getPid());
        }
        for (Objects o : objects2) {
            pids2.put(o.getId(), o.getPid());
        }

        // sql statements to put pairs into tabulation
        StringBuilder sb = new StringBuilder();
        for (Entry<String, Pair> p : map.entrySet()) {
            if (p1.get(p.getValue().v1) != null && p2.get(p.getValue().v2) != null) {
                String sql = "INSERT INTO tabulation (fid1, fid2, pid1, pid2, the_geom, area, occurrences, species) VALUES " +
                        "('" + fieldId1 + "','" + fieldId2 + "'," + "'" +
                        pids1.get(p.getValue().v1) + "','" +
                        pids2.get(p.getValue().v2) + "'," +
                        "ST_GEOMFROMTEXT('" + p.getValue().geom.toText() + "', 4326)," +
                        p.getValue().area + "," +
                        p.getValue().occurrences + "," +
                        p.getValue().species.cardinality() + ");";

                //sql can get large due to WKT. Execute here.
                jdbcTemplate.update(sql);
            }
        }
    }

    /**
     * Determine the grid resolution that will be in use.
     *
     * @param layers     list of layers to be used as String []
     * @param resolution target resolution as String
     * @return resolution that will be used
     */
    private static String confirmResolution(String[] layers, String resolution) {
        try {
            TreeMap<Double, String> resolutions = new TreeMap<Double, String>();
            for (String layer : layers) {
                String path = getLayerPath(resolution, layer);
                int end, start;
                if (path != null && ((end = path.lastIndexOf(File.separator)) > 0) && ((start = path.lastIndexOf(File.separator, end - 1)) > 0)) {
                    String res = path.substring(start + 1, end);
                    Double d = Double.parseDouble(res);
                    if (d < 1) {
                        resolutions.put(d, res);
                    }
                }
            }
            if (resolutions.size() > 0) {
                resolution = resolutions.firstEntry().getValue();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resolution;
    }

    static double[][] internalExtents(double[][] e1, double[][] e2) {
        double[][] internalExtents = new double[2][2];

        internalExtents[0][0] = Math.max(e1[0][0], e2[0][0]);
        internalExtents[0][1] = Math.max(e1[0][1], e2[0][1]);
        internalExtents[1][0] = Math.min(e1[1][0], e2[1][0]);
        internalExtents[1][1] = Math.min(e1[1][1], e2[1][1]);

        return internalExtents;
    }

//    private static byte[][] getMask(double res, double[][] extents, int w, int h) {
//        byte[][] mask = new byte[h][w];
//        for (int i = 0; i < h; i++) {
//            for (int j = 0; j < w; j++) {
//                mask[i][j] = 1;
//            }
//        }
//        return mask;
//    }

    static boolean isValidExtents(double[][] e) {
        return e[0][0] < e[1][0] && e[0][1] < e[1][1];
    }

    static double[][] getLayerExtents(String resolution, String layer) {
        double[][] extents = new double[2][2];
        Grid g = Grid.getGrid(getLayerPath(resolution, layer));

        extents[0][0] = g.xmin;
        extents[0][1] = g.ymin;
        extents[1][0] = g.xmax;
        extents[1][1] = g.ymax;

        return extents;
    }

    public static String getLayerPath(String resolution, String layer) {
        String analysisLayerDir = Client.getLayerIntersectDao().getConfig().getAnalysisLayerFilesPath();
        String field = getFieldId(layer);

        File file = new File(analysisLayerDir + File.separator + resolution + File.separator + field + ".grd");

        // move up a resolution when the file does not exist at the target
        // resolution
        try {
            while (!file.exists()) {
                TreeMap<Double, String> resolutionDirs = new TreeMap<Double, String>();
                for (File dir : new File(analysisLayerDir).listFiles()) {
                    if (dir.isDirectory()) {
                        try {
                            System.out.println(dir.getName());
                            resolutionDirs.put(Double.parseDouble(dir.getName()), dir.getName());
                        } catch (Exception e) {
                        }
                    }
                }

                String newResolution = resolutionDirs.higherEntry(Double.parseDouble(resolution)).getValue();

                if (newResolution.equals(resolution)) {
                    break;
                } else {
                    resolution = newResolution;
                    file = new File(analysisLayerDir + File.separator + resolution + File.separator + field + ".grd");
                }
            }
        } catch (Exception e) {
        }

        String layerPath = analysisLayerDir + File.separator + resolution + File.separator + field;

        if (new File(layerPath + ".grd").exists()) {
            return layerPath;
        } else {
            // look for an analysis layer
            System.out.println("getLayerPath, not a default layer, checking analysis output for: " + layer);
            String[] info = Client.getLayerIntersectDao().getConfig().getAnalysisLayerInfo(layer);
            if (info != null) {
                return info[1];
            } else {
                System.out.println("getLayerPath, cannot find for: " + layer + ", " + resolution);
                return null;
            }
        }
    }

    public static String getFieldId(String layerShortName) {
        String field = layerShortName;
        // layer short name -> layer id -> field id
        try {
            String id = String.valueOf(Client.getLayerDao().getLayerByName(layerShortName).getId());
            for (Field f : Client.getFieldDao().getFields()) {
                if (f.getSpid() != null && f.getSpid().equals(id)) {
                    field = f.getId();
                    break;
                }
            }
        } catch (Exception e) {
        }
        return field;
    }

    // private static void updateSingleObjects() {
    // Connection conn = null;
    // try {
    // conn = getConnection();
    // String allFidPairs =
    // "SELECT id as fid1 FROM fields f WHERE f.intersect=true";
    // String existingFidPairs =
    // "SELECT fid1 FROM tabulation WHERE fid2 = '' GROUP BY fid1";
    // String newFidPairs = "SELECT a.fid1 FROM (" + allFidPairs +
    // ") a LEFT JOIN (" + existingFidPairs + ") b "
    // + "ON a.fid1=b.fid1 WHERE b.fid1 is null group by a.fid1";
    // String sql = newFidPairs;
    // Statement s1 = conn.createStatement();
    // ResultSet rs1 = s1.executeQuery(sql);
    //
    // LinkedBlockingQueue<String> data = new LinkedBlockingQueue<String>();
    // while (rs1.next()) {
    // data.put(rs1.getString("fid1"));
    // }
    //
    // System.out.println("next " + data.size());
    //
    // int size = data.size();
    //
    // if (size == 0) {
    // return;
    // }
    //
    // CountDownLatch cdl = new CountDownLatch(data.size());
    //
    // SingleDistributionThread[] threads = new
    // SingleDistributionThread[CONCURRENT_THREADS];
    // for (int j = 0; j < CONCURRENT_THREADS; j++) {
    // threads[j] = new
    // SingleDistributionThread(getConnection().createStatement(), data, cdl);
    // threads[j].start();
    // }
    //
    // cdl.await();
    //
    // for (int j = 0; j < CONCURRENT_THREADS; j++) {
    // try {
    // threads[j].s.getConnection().close();
    // } catch (Exception e) {
    // e.printStackTrace();
    // }
    // threads[j].interrupt();
    // }
    //
    // } catch (Exception ex) {
    // ex.printStackTrace();
    // } finally {
    // if(conn != null) {
    // try {
    // conn.close();
    // } catch (Exception e) {
    // e.printStackTrace();
    // }
    // }
    // }
    // }
    private static int updateArea() {
        Connection conn = null;
        try {
            conn = getConnection();
            String sql = "SELECT pid1, pid2, ST_AsText(the_geom) as wkt FROM tabulation WHERE pid1 is not null AND area is null " + " limit 100";
            if (conn == null) {
                System.out.println("connection is null");
            } else {
                System.out.println("connection is not null");
            }
            Statement s1 = conn.createStatement();
            ResultSet rs1 = s1.executeQuery(sql);

            ConcurrentLinkedQueue<String[]> data = new ConcurrentLinkedQueue<String[]>();
            while (rs1.next()) {
                data.add(new String[]{rs1.getString("pid1"), rs1.getString("pid2"), rs1.getString("wkt")});
            }

            System.out.println("next " + data.size());

            int size = data.size();

            if (size == 0) {
                return 0;
            }

            AreaThread[] threads = new AreaThread[CONCURRENT_THREADS];
            for (int j = 0; j < CONCURRENT_THREADS; j++) {
                threads[j] = new AreaThread(data, getConnection().createStatement());
                threads[j].start();
            }

            return size;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return 0;
    }

    public static int updateOccurrencesSpecies2(Records records, int threadCount, String path) {
        FieldDAO fieldDao = Client.getFieldDao();
        LayerDAO layerDao = Client.getLayerDao();
        ObjectDAO objectDao = Client.getObjectDao();
        LayerIntersectDAO layerIntersectDao = Client.getLayerIntersectDao();

        // reduce points
        HashSet<String> uniquePoints = new HashSet<String>();
        for (int i = 0; i < records.getRecordsSize(); i++) {
            uniquePoints.add(records.getLongitude(i) + " " + records.getLatitude(i));
        }
        ArrayList<String> pts = new ArrayList<String>(uniquePoints);
        java.util.Collections.sort(pts);
        uniquePoints = null;
        double[][] points = new double[pts.size()][2];
        for (int i = 0; i < points.length; i++) {
            String[] p = pts.get(i).split(" ");
            points[i][0] = Double.NaN;
            points[i][1] = Double.NaN;
            try {
                points[i][0] = Double.parseDouble(p[0]);
                points[i][1] = Double.parseDouble(p[1]);
            } catch (Exception e) {
            }
        }

        int[] pointIdx = new int[records.getRecordsSize()];
        for (int i = 0; i < records.getRecordsSize(); i++) {
            pointIdx[i] = java.util.Collections.binarySearch(pts, records.getLongitude(i) + " " + records.getLatitude(i));
        }

        ArrayList<Field> fields = new ArrayList<Field>();
        ArrayList<File> files = new ArrayList<File>();

        // perform sampling, only for layers with a shape file requiring an
        // intersection
        for (Field f : fieldDao.getFields()) {
            //create new sampling file when one does not already exist
            if (f.isIntersect() && (path == null || !(new File(path + "_sample_" + f.getId()).exists()))) {
                try {
                    String fieldName = f.getSid();
                    Layer l = layerDao.getLayerById(Integer.valueOf(f.getSpid()));
                    String filename = layerIntersectDao.getConfig().getLayerFilesPath() + File.separator + l.getPath_orig();

                    System.out.println(filename);

                    //shapefile
                    File shp = new File(filename + ".shp");

                    if (shp.exists()) {
                        SimpleShapeFile ssf = null;
                        if (layerIntersectDao.getConfig().getShapeFileCache() != null) {
                            ssf = layerIntersectDao.getConfig().getShapeFileCache().get(filename);
                        }
                        if (ssf == null) {
                            ssf = new SimpleShapeFile(filename, fieldName);
                        }

                        String[] catagories;
                        int column_idx = ssf.getColumnIdx(fieldName);
                        catagories = ssf.getColumnLookup(column_idx);
                        int[] values = ssf.intersect(points, catagories, column_idx, threadCount);

                        // catagories to pid
                        List<Objects> objects = objectDao.getObjectsById(f.getId());
                        int[] catToPid = new int[catagories.length];
                        for (int j = 0; j < objects.size(); j++) {
                            for (int i = 0; i < catagories.length; i++) {
                                if ((catagories[i] == null || objects.get(j).getId() == null) && catagories[i] == objects.get(j).getId()) {
                                    catToPid[i] = j;
                                    break;
                                } else if (catagories[i] != null && objects.get(j).getId() != null && catagories[i].compareTo(objects.get(j).getId()) == 0) {
                                    catToPid[i] = j;
                                    break;
                                }
                            }
                        }

                        // export pids in points order
                        FileWriter fw = null;
                        try {
                            File tmp = null;
                            if (path == null) {
                                tmp = File.createTempFile(f.getId(), "tabulation_generator");
                            } else {
                                tmp = new File(path + "/sample_" + f.getId());
                            }
                            System.out.println("**** tmp file **** > " + tmp.getPath());
                            fields.add(f);
                            files.add(tmp);
                            fw = new FileWriter(tmp);
                            if (values != null) {
                                for (int i = 0; i < values.length; i++) {
                                    if (i > 0) {
                                        fw.append("\n");
                                    }
                                    if (values[i] >= 0) {
                                        fw.append(objects.get(catToPid[values[i]]).getPid());
                                    } else {
                                        fw.append("n/a");
                                    }
                                }
                            }
                            System.out.println("**** OK ***** > " + l.getPath_orig());
                        } catch (Exception e) {
                            System.out.println("problem with sampling: " + l.getPath_orig());
                            e.printStackTrace();
                        } finally {
                            if (fw != null) {
                                try {
                                    fw.close();
                                } catch (Exception e) {
                                }
                            }
                        }
                    } else {
                        //grid as shp
                        Grid g = new Grid(layerIntersectDao.getConfig().getLayerFilesPath() + File.separator
                                + l.getPath_orig());
                        if (g != null) {
                            float[] values = g.getValues(points);

                            // export pids in points order
                            FileWriter fw = null;
                            try {
                                File tmp = null;
                                if (path == null) {
                                    tmp = File.createTempFile(f.getId(), "tabulation_generator");
                                } else {
                                    tmp = new File(path + "/sample_" + f.getId());
                                }
                                System.out.println("**** tmp file **** > " + tmp.getPath());
                                fields.add(f);
                                files.add(tmp);
                                fw = new FileWriter(tmp);
                                if (values != null) {
                                    for (int i = 0; i < values.length; i++) {
                                        if (i > 0) {
                                            fw.append("\n");
                                        }
                                        if (values[i] >= 0) {
                                            fw.append(String.valueOf(values[i]));
                                        } else {
                                            fw.append("n/a");
                                        }
                                    }
                                }
                                System.out.println("**** OK ***** > " + l.getPath_orig());
                            } catch (Exception e) {
                                System.out.println("problem with sampling: " + l.getPath_orig());
                                e.printStackTrace();
                            } finally {
                                if (fw != null) {
                                    try {
                                        fw.close();
                                    } catch (Exception e) {
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("problem with sampling: " + f.getId());
                    e.printStackTrace();
                }
            }
        }

        // evaluate and write
        Connection conn = null;
        try {
            conn = getConnection();
            Statement statement = conn.createStatement();

            // operate on each pid pair
            for (int i = 0; i < fields.size(); i++) {
                // load file for i
                String[] s1 = loadFile(files.get(i), pts.size());

                for (int j = i + 1; j < fields.size(); j++) {
                    // load file for j
                    String[] s2 = loadFile(files.get(j), pts.size());

                    // compare
                    ArrayList<String> sqlUpdates = compare(records, pointIdx, s1, s2);

                    // batch
                    StringBuilder sb = new StringBuilder();
                    for (String s : sqlUpdates) {
                        sb.append(s).append(";\n");
                    }

                    // commit
                    statement.execute(sb.toString());
                    System.out.println(sb.toString());
                }
            }

            // set nulls
            statement.execute("UPDATE tabulation SET occurrences=0 WHERE occurrences is null;");
            statement.execute("UPDATE tabulation SET species=0 WHERE species is null;");

            if (path == null) {
                for (int i = 0; i < files.size(); i++) {
                    System.out.println("FILE: " + files.get(i).getPath());
                    files.get(i).delete();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static int updateOccurrencesSpecies(Records records, int threadCount, String path, String fid1, String fid2) {
        FieldDAO fieldDao = Client.getFieldDao();
        LayerDAO layerDao = Client.getLayerDao();
        ObjectDAO objectDao = Client.getObjectDao();
        LayerIntersectDAO layerIntersectDao = Client.getLayerIntersectDao();

        // reduce points
        HashSet<String> uniquePoints = new HashSet<String>();
        for (int i = 0; i < records.getRecordsSize(); i++) {
            uniquePoints.add(records.getLongitude(i) + " " + records.getLatitude(i));
        }
        ArrayList<String> pts = new ArrayList<String>(uniquePoints);
        java.util.Collections.sort(pts);
        uniquePoints = null;
        double[][] points = new double[pts.size()][2];
        for (int i = 0; i < points.length; i++) {
            String[] p = pts.get(i).split(" ");
            points[i][0] = Double.NaN;
            points[i][1] = Double.NaN;
            try {
                points[i][0] = Double.parseDouble(p[0]);
                points[i][1] = Double.parseDouble(p[1]);
            } catch (Exception e) {
            }
        }

        int[] pointIdx = new int[records.getRecordsSize()];
        for (int i = 0; i < records.getRecordsSize(); i++) {
            pointIdx[i] = java.util.Collections.binarySearch(pts, records.getLongitude(i) + " " + records.getLatitude(i));
        }

        ArrayList<Field> fields = new ArrayList<Field>();
        ArrayList<File> files = new ArrayList<File>();

        // perform sampling, only for layers with a shape file requiring an
        // intersection
        for (Field f : fieldDao.getFields()) {
            if (f.getId().equals(fid1) || f.getId().equals(fid2)) {
                //create new sampling file when one does not already exist
                if (f.isIntersect() && (path == null || !(new File(path + "_sample_" + f.getId()).exists()))) {
                    try {
                        String fieldName = f.getSid();
                        Layer l = layerDao.getLayerById(Integer.valueOf(f.getSpid()));
                        String filename = layerIntersectDao.getConfig().getLayerFilesPath() + File.separator + l.getPath_orig();

                        System.out.println(filename);

                        //shapefile
                        File shp = new File(filename + ".shp");

                        if (shp.exists()) {
                            SimpleShapeFile ssf = null;
                            if (layerIntersectDao.getConfig().getShapeFileCache() != null) {
                                ssf = layerIntersectDao.getConfig().getShapeFileCache().get(filename);
                            }
                            if (ssf == null) {
                                ssf = new SimpleShapeFile(filename, fieldName);
                            }

                            String[] catagories;
                            int column_idx = ssf.getColumnIdx(fieldName);
                            catagories = ssf.getColumnLookup(column_idx);
                            int[] values = ssf.intersect(points, catagories, column_idx, threadCount);

                            // catagories to pid
                            List<Objects> objects = objectDao.getObjectsById(f.getId());
                            int[] catToPid = new int[catagories.length];
                            for (int j = 0; j < objects.size(); j++) {
                                for (int i = 0; i < catagories.length; i++) {
                                    if ((catagories[i] == null || objects.get(j).getId() == null) && catagories[i] == objects.get(j).getId()) {
                                        catToPid[i] = j;
                                        break;
                                    } else if (catagories[i] != null && objects.get(j).getId() != null && catagories[i].compareTo(objects.get(j).getId()) == 0) {
                                        catToPid[i] = j;
                                        break;
                                    }
                                }
                            }

                            // export pids in points order
                            FileWriter fw = null;
                            try {
                                File tmp = null;
                                if (path == null) {
                                    tmp = File.createTempFile(f.getId(), "tabulation_generator");
                                } else {
                                    tmp = new File(path + "/sample_" + f.getId());
                                }
                                System.out.println("**** tmp file **** > " + tmp.getPath());
                                fields.add(f);
                                files.add(tmp);
                                fw = new FileWriter(tmp);
                                if (values != null) {
                                    for (int i = 0; i < values.length; i++) {
                                        if (i > 0) {
                                            fw.append("\n");
                                        }
                                        if (values[i] >= 0) {
                                            fw.append(objects.get(catToPid[values[i]]).getPid());
                                        } else {
                                            fw.append("n/a");
                                        }
                                    }
                                }
                                System.out.println("**** OK ***** > " + l.getPath_orig());
                            } catch (Exception e) {
                                System.out.println("problem with sampling: " + l.getPath_orig());
                                e.printStackTrace();
                            } finally {
                                if (fw != null) {
                                    try {
                                        fw.close();
                                    } catch (Exception e) {
                                    }
                                }
                            }
                        } else {
                            //grid as shp
                            Grid g = new Grid(layerIntersectDao.getConfig().getLayerFilesPath() + File.separator
                                    + l.getPath_orig());
                            if (g != null) {
                                float[] values = g.getValues(points);

                                // export pids in points order
                                FileWriter fw = null;
                                try {
                                    File tmp = null;
                                    if (path == null) {
                                        tmp = File.createTempFile(f.getId(), "tabulation_generator");
                                    } else {
                                        tmp = new File(path + "/sample_" + f.getId());
                                    }
                                    System.out.println("**** tmp file **** > " + tmp.getPath());
                                    fields.add(f);
                                    files.add(tmp);
                                    fw = new FileWriter(tmp);
                                    if (values != null) {
                                        for (int i = 0; i < values.length; i++) {
                                            if (i > 0) {
                                                fw.append("\n");
                                            }
                                            if (values[i] >= 0) {
                                                fw.append(String.valueOf(values[i]));
                                            } else {
                                                fw.append("n/a");
                                            }
                                        }
                                    }
                                    System.out.println("**** OK ***** > " + l.getPath_orig());
                                } catch (Exception e) {
                                    System.out.println("problem with sampling: " + l.getPath_orig());
                                    e.printStackTrace();
                                } finally {
                                    if (fw != null) {
                                        try {
                                            fw.close();
                                        } catch (Exception e) {
                                        }
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("problem with sampling: " + f.getId());
                        e.printStackTrace();
                    }
                }
            }
        }

        // evaluate and write
        Connection conn = null;
        try {
            conn = getConnection();
            Statement statement = conn.createStatement();

            // operate on each pid pair
            for (int i = 0; i < fields.size(); i++) {
                // load file for i
                String[] s1 = loadFile(files.get(i), pts.size());

                for (int j = i + 1; j < fields.size(); j++) {
                    // load file for j
                    String[] s2 = loadFile(files.get(j), pts.size());

                    // compare
                    ArrayList<String> sqlUpdates = compare(records, pointIdx, s1, s2);

                    // batch
                    StringBuilder sb = new StringBuilder();
                    for (String s : sqlUpdates) {
                        sb.append(s).append(";\n");
                    }

                    // commit
                    statement.execute(sb.toString());
                    System.out.println(sb.toString());
                }
            }

            // set nulls
            statement.execute("UPDATE tabulation SET occurrences=0 WHERE occurrences is null;");
            statement.execute("UPDATE tabulation SET species=0 WHERE species is null;");

            if (path == null) {
                for (int i = 0; i < files.size(); i++) {
                    System.out.println("FILE: " + files.get(i).getPath());
                    files.get(i).delete();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    static String[] loadFile(File f, int size) {
        String[] s = new String[size];
        try {
            BufferedReader br = new BufferedReader(new FileReader(f));
            String line;
            int i = 0;
            while ((line = br.readLine()) != null) {
                s[i] = line;
                i++;
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return s;
    }

    private static void deleteInvalidObjects() {
        Connection conn = null;
        try {
            String sql = "delete from tabulation where the_geom is not null and st_area(the_geom) = 0;";
            conn = getConnection();
            conn.createStatement().execute(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static ArrayList<String> compare(Records records, int[] pointIdx, String[] s1, String[] s2) {
        ArrayList<String> sqlUpdates = new ArrayList<String>();
        BitSet bitset;
        Integer count;
        String key;
        HashMap<String, BitSet> species = new HashMap<String, BitSet>();
        HashMap<String, Integer> occurrences = new HashMap<String, Integer>();

        for (int i = 0; i < pointIdx.length; i++) {
            key = s1[pointIdx[i]] + " " + s2[pointIdx[i]];

            bitset = species.get(key);
            if (bitset == null) {
                bitset = new BitSet();
            }
            bitset.set(records.getSpeciesNumber(i));
            species.put(key, bitset);

            count = occurrences.get(key);
            if (count == null) {
                count = 0;
            }
            count = count + 1;
            occurrences.put(key, count);
        }

        // produce sql update statements
        for (String k : species.keySet()) {
            String[] pids = k.split(" ");
            sqlUpdates.add("UPDATE tabulation SET " + "species = " + species.get(k).cardinality() + ", " + "occurrences = " + occurrences.get(k) + " WHERE (pid1='" + pids[0] + "' AND pid2='"
                    + pids[1] + "') " + "OR (pid1='" + pids[1] + "' AND pid2='" + pids[0] + "')");
        }

        return sqlUpdates;
    }

    public static List<Tabulation> calc(String fid, String wkt) {
        List<Tabulation> tabulations = new ArrayList<Tabulation>();

        // prep grid file
        IntersectionFile f = Client.getLayerIntersectDao().getConfig().getIntersectionFile(fid);
        Grid grid1 = new Grid(f.getFilePath());
        double resolution = grid1.xres;

        // get extents for all layers
        double[][] field1Extents = new double[2][2];
        field1Extents[0][0] = grid1.xmin;
        field1Extents[1][0] = grid1.xmax;
        field1Extents[0][1] = grid1.ymin;
        field1Extents[1][1] = grid1.ymax;
        System.out.println("Extents for " + fid + ": " + field1Extents);

        SimpleRegion sr = SimpleShapeFile.parseWKT(wkt);
        double[][] field2Extents = sr.getBoundingBox();

        double[][] extents = internalExtents(field1Extents, field2Extents);
        System.out.println("Internal extents: " + extents);
        if (!isValidExtents(extents)) {
            System.out.println("Warning, no overlap between grids: " + fid);
            return tabulations;
        }

        // get mask and adjust extents for filter
        int width = 0, height = 0;
        System.out.println("resolution: " + resolution);
        height = (int) Math.ceil((extents[1][1] - extents[0][1]) / resolution);
        width = (int) Math.ceil((extents[1][0] - extents[0][0]) / resolution);


        //grid1.getGrid();
        Properties p1 = new Properties();
        try {
            p1.load(new FileReader(f.getFilePath() + ".txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        HashMap<String, Pair> map = new HashMap<String, Pair>();

        // build intersections by category pairs
        for (int i = 0; i < width; i++) {
            for (int j = 0; j < height; j++) {
                if (sr.isWithin(extents[0][0] + resolution * i, extents[0][1] + resolution * j)) {
                    // area
                    int v1 = (int) grid1.getValues3(new double[][]{{extents[0][0] + resolution * i, extents[0][1] + resolution * j}}, 1024 * 1024)[0];
                    String key = v1 + " " + v1;
                    Pair p = map.get(key);
                    if (p == null) {
                        p = new Pair(key);
                        map.put(key, p);
                    }
                    p.area += SpatialUtil.cellArea(resolution, extents[0][1] + resolution * j) * 1000000; // convert
                    // sqkm
                    // to
                    // sqm
                }
            }
        }

        // sql statements to put pairs into tabulation
        for (Entry<String, Pair> p : map.entrySet()) {
            if (p1.get(p.getValue().v1) != null) {
                Tabulation t = new Tabulation();
                t.setFid1(fid);
                t.setName1(p1.getProperty(p.getValue().v1));
                t.setFid2("");
                t.setArea(p.getValue().area);
                t.setPid1(p.getValue().v1);
                t.setPid2("");

                tabulations.add(t);
            }
        }

        return tabulations;
    }
}

class DistributionThread extends Thread {

    ConcurrentLinkedQueue<String> queue;
    CountDownLatch cdl;

    public DistributionThread(ConcurrentLinkedQueue<String> queue) {
        this.queue = queue;
    }

    @Override
    public void run() {
        String f;
        try {
            while ((f = queue.poll()) != null) {

                String fid1 = f.split(",")[0];
                String fid2 = f.split(",")[1];

                String sql = "INSERT INTO tabulation (fid1, pid1, fid2, pid2, the_geom) "
                        + "SELECT '" + fid1 + "', o1.pid, '" + fid2 + "', o2.pid, "
                        + "ST_INTERSECTION(o1.the_geom, o2.the_geom)"
                        + "FROM (select * from objects where fid='" + fid1 + "') o1 INNER JOIN "
                        + "(select * from objects where fid='" + fid2 + "') o2 ON "
                        + "ST_Intersects(ST_ENVELOPE(o1.the_geom), ST_ENVELOPE(o2.the_geom));";
                //fetch

                System.out.println("start: " + fid1 + "," + fid2);
                long start = System.currentTimeMillis();
                PreparedStatement ps = TabulationGenerator.getConnection().prepareStatement(sql);
                int update = ps.executeUpdate();
                long end = System.currentTimeMillis();
                System.out.println("processed: " + fid1 + "," + fid2 + " in " + (end - start) / 1000 + "s (" + update + ") rows");
                System.out.println(sql);
                ps.close();

            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}

//class SingleDistributionThread extends Thread {
//
//    Statement s;
//    LinkedBlockingQueue<String> lbq;
//    CountDownLatch cdl;
//
//    public SingleDistributionThread(Statement s, LinkedBlockingQueue<String> lbq, CountDownLatch cdl) {
//        this.s = s;
//        this.lbq = lbq;
//        this.cdl = cdl;
//    }
//
//    @Override
//    public void run() {
//        while (true) {
//            try {
//                String fid = lbq.take();
//
//                String single_column_sql = "INSERT INTO tabulation (fid1, pid1, fid2, pid2, the_geom) " + "SELECT fid, pid, '', '', the_geom FROM objects WHERE fid='" + fid + "'";
//
//                s.executeUpdate(single_column_sql);
//
//            } catch (Exception ex) {
//                ex.printStackTrace();
//            }
//            cdl.countDown();
//        }
//    }
//}

class AreaThread extends Thread {

    Statement s;
    ConcurrentLinkedQueue<String[]> queue;

    public AreaThread(ConcurrentLinkedQueue<String[]> queue, Statement s) {
        this.s = s;
        this.queue = queue;
    }

    @Override
    public void run() {
        try {
            String[] data;
            while ((data = queue.poll()) != null) {
                double area = SpatialUtil.calculateArea(data[2]);

                String sql = "UPDATE tabulation SET area = " + area + " WHERE pid1='" + data[0] + "' AND pid2='" + data[1] + "';";

                int update = s.executeUpdate(sql);
            }
            s.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

/*
 * class OccurrencesSpeciesThread extends Thread {
 * 
 * Statement s; LinkedBlockingQueue<String[]> lbq; CountDownLatch cdl;
 * 
 * public OccurrencesSpeciesThread(LinkedBlockingQueue<String[]> lbq,
 * CountDownLatch cdl, Statement s) { this.s = s; this.cdl = cdl; this.lbq =
 * lbq; }
 * 
 * @Override public void run() { try { while (true) {
 * 
 * try { String[] data = lbq.take(); Records records = new
 * Records("/Users/fan03c/biochache_records/biochache_records.csv", data[2]);
 * int occurrences = records.getRecordsSize(); int species =
 * records.getSpeciesSize();
 * 
 * String sqlUpdate = "UPDATE tabulation SET occurrences = " + occurrences +
 * ", species = " + species + " WHERE pid1='" + data[0] + "' AND pid2='" +
 * data[1] + "';";
 * 
 * int update = s.executeUpdate(sqlUpdate);
 * 
 * } catch (InterruptedException e) { break; } catch (Exception e) {
 * e.printStackTrace(); } cdl.countDown(); } } catch (Exception e) {
 * e.printStackTrace(); } } }
 */

//class OccurrencesSpeciesThread extends Thread {
//
//    Statement s;
//    LinkedBlockingQueue<String[]> lbq;
//    CountDownLatch cdl;
//    Records r;
//
//    public OccurrencesSpeciesThread(LinkedBlockingQueue<String[]> lbq, CountDownLatch cdl, Statement s, Records r) {
//        this.s = s;
//        this.cdl = cdl;
//        this.lbq = lbq;
//        this.r = r;
//    }
//
//    @Override
//    public void run() {
//        try {
//            while (true) {
//
//                try {
//                    String[] data = lbq.take();
//
//                    SimpleRegion areaWorking = SimpleShapeFile.parseWKT(data[2]);
//                    int recordsLength = r.getRecordsSize();
//                    int occurrences = 0;
//
//                    /*
//                     * ArrayList<String> speciesName = new ArrayList<String>();
//                     * for (int i = 0;i < recordsLength;i++){ double longitude =
//                     * r.getLongitude(i); double latitude = r.getLatitude(i); if
//                     * (areaWorking.isWithin(longitude, latitude)){
//                     * occurrences++; if
//                     * (speciesName.contains(r.getSpecies(i))==false){
//                     * speciesName.add(r.getSpecies(i)); } } } int species =
//                     * speciesName.size();
//                     */
//                    BitSet speciesSet = new BitSet(r.getSpeciesSize());
//                    for (int i = 0; i < recordsLength; i++) {
//                        double longitude = r.getLongitude(i);
//                        double latitude = r.getLatitude(i);
//                        if (areaWorking.isWithin(longitude, latitude)) {
//                            occurrences++;
//                            speciesSet.set(r.getSpeciesNumber(i));
//                        }
//                    }
//                    int species = speciesSet.cardinality();
//
//                    String sqlUpdate = "UPDATE tabulation SET occurrences = " + occurrences + ", species = " + species + " WHERE pid1='" + data[0] + "' AND pid2='" + data[1] + "';";
//
//                    int update = s.executeUpdate(sqlUpdate);
//
//                } catch (InterruptedException e) {
//                    break;
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                cdl.countDown();
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}

class Pair {

    String key;
    int occurrences;
    BitSet species = new BitSet();
    double area;
    String v1, v2;
    Geometry geom;

    Pair(String key) {
        this.key = key;
        String[] split = key.split(" ");
        v1 = split[0];
        v2 = split[1];
    }
}
