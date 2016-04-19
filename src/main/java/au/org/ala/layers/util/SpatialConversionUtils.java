package au.org.ala.layers.util;

import com.google.common.io.Files;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.GeometryType;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.*;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/**
 * Utilities for converting spatial data between formats
 *
 * @author ChrisF
 */
public class SpatialConversionUtils {

    public final static String WKT_MAP_KEY = "WKT_MAP_KEY_****"; //works as long as this is not uploaded as a field in the shapefile
    /**
     * log4j logger
     */
    private static final Logger logger = Logger.getLogger(SpatialConversionUtils.class);

    public static List<String> getGeometryCollectionParts(String wkt) {
        if (wkt.matches("GEOMETRYCOLLECTION\\(.+\\)")) {
            String parts = wkt.substring(19, wkt.length() - 1);

            int bracketLevel = 0;
            List<Integer> commaPositions = new ArrayList<Integer>();

            for (int i = 0; i < parts.length(); i++) {
                char c = parts.charAt(i);

                if (c == '(') {
                    bracketLevel++;
                } else if (c == ')') {
                    bracketLevel--;
                } else if (c == ',' && bracketLevel == 0) {
                    commaPositions.add(i);
                }
            }

            List<String> partsList = new ArrayList<String>();

            if (commaPositions.size() == 0) {
                partsList.add(parts);
            } else {
                int lastUsedCommaPosition = 0;
                for (int i = 0; i < commaPositions.size(); i++) {
                    int commaPosition = commaPositions.get(i);
                    if (i == 0) {
                        partsList.add(parts.substring(0, commaPosition));
                        lastUsedCommaPosition = commaPosition;
                    } else {
                        partsList.add(parts.substring(lastUsedCommaPosition + 1, commaPosition));
                        lastUsedCommaPosition = commaPosition;
                    }

                    if (i == commaPositions.size() - 1) {
                        partsList.add(parts.substring(commaPosition + 1));
                        lastUsedCommaPosition = commaPosition;
                    }
                }
            }
            return partsList;
        } else {
            throw new IllegalArgumentException("Invalid input. Expecting a valid GEOMETRYCOLLECTION wkt string.");
        }
    }

    public static boolean isWKTValid(String wkt) {
        WKTReader wktReader = new WKTReader();
        try {
            Geometry geom = wktReader.read(wkt);
            return geom.isValid();
        } catch (ParseException ex) {
            return false;
        }
    }

    public static String geoJsonToWkt(String geoJson) throws IOException {
        GeometryJSON gJson = new GeometryJSON();
        Geometry geometry = gJson.read(new StringReader(geoJson));

        if (!geometry.isValid()) {
            return null;
        }

        String wkt = geometry.toText();
        return wkt;
    }

    public static Pair<String, File> extractZippedShapeFile(File zippedShpFile) throws IOException {

        File tempDir = Files.createTempDir();

        // Unpack the zipped shape file into the temp directory
        ZipFile zf = null;
        File shpFile = null;
        try {
            zf = new ZipFile(zippedShpFile);

            boolean shpPresent = false;
            boolean shxPresent = false;
            boolean dbfPresent = false;

            Enumeration<? extends ZipEntry> entries = zf.entries();

            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                InputStream inStream = zf.getInputStream(entry);
                File f = new File(tempDir, entry.getName());
                if (!f.getName().startsWith(".")) {
                    if (entry.isDirectory()) {
                        f.mkdirs();
                    } else {
                        FileOutputStream outStream = new FileOutputStream(f);
                        IOUtils.copy(inStream, outStream);

                        if (entry.getName().endsWith(".shp")) {
                            shpPresent = true;
                            shpFile = f;
                        } else if (entry.getName().endsWith(".shx") && !f.getName().startsWith("/")) {
                            shxPresent = true;
                        } else if (entry.getName().endsWith(".dbf") && !f.getName().startsWith("/")) {
                            dbfPresent = true;
                        }
                    }
                }
            }

            if (!shpPresent || !shxPresent || !dbfPresent) {
                throw new IllegalArgumentException("Invalid archive. Must contain .shp, .shx and .dbf at a minimum.");
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (zf != null) {
                try {
                    zf.close();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }

        if (shpFile == null) {
            return null;
        } else {
            return Pair.of(shpFile.getParentFile().getName(), shpFile);
        }
    }

    public static List<List<Pair<String, Object>>> getShapeFileManifest(File shpFile) throws IOException {
        List<List<Pair<String, Object>>> manifestData = new ArrayList<List<Pair<String, Object>>>();

        FileDataStore store = FileDataStoreFinder.getDataStore(shpFile);

        SimpleFeatureSource featureSource = store.getFeatureSource(store.getTypeNames()[0]);
        SimpleFeatureCollection featureCollection = featureSource.getFeatures();
        SimpleFeatureIterator it = featureCollection.features();

        while (it.hasNext()) {
            SimpleFeature feature = (SimpleFeature) it.next();
            List<Pair<String, Object>> pairList = new ArrayList<Pair<String, Object>>();
            for (Property prop : feature.getProperties()) {
                if (!(prop.getType() instanceof GeometryType)) {
                    Pair<String, Object> pair = Pair.of(prop.getName().toString(), feature.getAttribute(prop.getName()));
                    pairList.add(pair);
                }
            }
            manifestData.add(pairList);
        }

//        it.close();

        return manifestData;
    }

    public static String getShapeFileFeatureAsWKT(File shpFileDir, int featureIndex) throws IOException {
        String wkt = null;

        if (!shpFileDir.exists() || !shpFileDir.isDirectory()) {
            throw new IllegalArgumentException("Supplied directory does not exist or is not a directory");
        }

        File shpFile = null;
        for (File f : shpFileDir.listFiles()) {
            if (f.getName().endsWith(".shp")) {
                shpFile = f;
                break;
            }
        }

        if (shpFile == null) {
            throw new IllegalArgumentException("No .shp file present in directory");
        }

        FileDataStore store = FileDataStoreFinder.getDataStore(shpFile);

        SimpleFeatureSource featureSource = store.getFeatureSource(store.getTypeNames()[0]);
        SimpleFeatureCollection featureCollection = featureSource.getFeatures();
        SimpleFeatureIterator it = featureCollection.features();

        //transform CRS to the same as the shapefile (at least try)
        //default to 4326
        CoordinateReferenceSystem crs = null;
        try {
            crs = store.getSchema().getCoordinateReferenceSystem();
            if (crs == null) {
                //attempt to parse prj
                try {
                    File prjFile = new File(shpFile.getPath().substring(0, shpFile.getPath().length() - 3) + "prj");
                    if (prjFile.exists()) {
                        String prj = FileUtils.readFileToString(prjFile);

                        if (prj.equals("PROJCS[\"WGS_1984_Web_Mercator_Auxiliary_Sphere\",GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\",SPHEROID[\"WGS_1984\",6378137.0,298.257223563]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Mercator_Auxiliary_Sphere\"],PARAMETER[\"False_Easting\",0.0],PARAMETER[\"False_Northing\",0.0],PARAMETER[\"Central_Meridian\",0.0],PARAMETER[\"Standard_Parallel_1\",0.0],PARAMETER[\"Auxiliary_Sphere_Type\",0.0],UNIT[\"Meter\",1.0]]")) {
                            //support for arcgis online default shp exports
                            crs = CRS.decode("EPSG:3857");
                        } else {
                            crs = CRS.parseWKT(FileUtils.readFileToString(prjFile));
                        }
                    }
                } catch (Exception e) {
                    logger.error("failed to read prj for " + shpFile.getPath());
                }

                if (crs == null) {
                    crs = DefaultGeographicCRS.WGS84;
                }
            }
        } catch (Exception e) {
            logger.error("error with CRS");
        }

        int i = 0;
        while (it.hasNext()) {
            SimpleFeature feature = (SimpleFeature) it.next();
            if (i == featureIndex) {
                Geometry g = (Geometry) feature.getDefaultGeometry();

                try {
                    wkt = JTS.transform(g, CRS.findMathTransform(crs, DefaultGeographicCRS.WGS84, true)).toString();
                } catch (Exception e) {
                    //log the error and continue anyway
                    logger.error("failed CRS transformation for: " + shpFile.getPath() + ", continuing with untransformed geometry", e);

                    wkt = g.toString();
                }

                break;
            }

            i++;
        }

        return wkt;
    }

    public static File buildZippedShapeFile(String wktString, String filenamePrefix, String name, String description) throws IOException {

        File tempDir = Files.createTempDir();

        File shpFile = new File(tempDir, filenamePrefix + ".shp");

        saveShapefile(shpFile, wktString, name, description);

        File zipFile = new File(tempDir, filenamePrefix + ".zip");
        ZipOutputStream zipOS = null;
        try {
            zipOS = new ZipOutputStream(new FileOutputStream(zipFile));

            List<File> excludedFiles = new ArrayList<File>();
            excludedFiles.add(zipFile);

            Iterator<File> iterFile = FileUtils.iterateFiles(shpFile.getParentFile(), new BaseFileNameInDirectoryFilter(filenamePrefix, tempDir, excludedFiles), null);

            while (iterFile.hasNext()) {
                File nextFile = iterFile.next();
                ZipEntry zipEntry = new ZipEntry(nextFile.getName());
                zipOS.putNextEntry(zipEntry);
                zipOS.write(FileUtils.readFileToByteArray(nextFile));
                zipOS.closeEntry();
            }
            zipOS.flush();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (zipOS != null) {
                try {
                    zipOS.close();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }

        return zipFile;
    }

    public static File saveShapefile(File shpfile, String wktString, String name, String description) {
        ShapefileDataStore newDataStore = null;
        try {
            String wkttype = "POLYGON";
            if (wktString.contains("MULTIPOLYGON")) {
                wkttype = "MULTIPOLYGON";
            } else if (wktString.contains("GEOMETRYCOLLECTION")) {
                wkttype = "GEOMETRYCOLLECTION";
            }
            final SimpleFeatureType TYPE = createFeatureType(wkttype);

            List<SimpleFeature> features = new ArrayList<SimpleFeature>();
            SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(TYPE);

            WKTReader wkt = new WKTReader();
            Geometry geom = wkt.read(wktString);

            if (geom instanceof GeometryCollection && !(geom instanceof MultiPolygon)) {
                GeometryCollection gc = (GeometryCollection) geom;
                for (int i = 0; i < gc.getNumGeometries(); i++) {
                    featureBuilder.add(gc.getGeometryN(i));
                    SimpleFeature feature = featureBuilder.buildFeature(null);

                    if (name != null) {
                        feature.setAttribute("name", name + " " + (i + 1));
                    }

                    if (description != null) {
                        feature.setAttribute("desc", description);
                    }

                    features.add(feature);
                }
            } else {
                featureBuilder.add(geom);
                SimpleFeature feature = featureBuilder.buildFeature(null);

                if (name != null) {
                    feature.setAttribute("name", name);
                }

                if (description != null) {
                    feature.setAttribute("desc", description);
                }

                features.add(feature);
            }

            ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();

            Map<String, Serializable> params = new HashMap<String, Serializable>();
            params.put("url", shpfile.toURI().toURL());
            params.put("create spatial index", Boolean.TRUE);

            newDataStore = (ShapefileDataStore) dataStoreFactory.createNewDataStore(params);
            newDataStore.createSchema(TYPE);

            newDataStore.forceSchemaCRS(DefaultGeographicCRS.WGS84);

            Transaction transaction = new DefaultTransaction("create");

            String typeName = newDataStore.getTypeNames()[0];
            SimpleFeatureSource featureSource = newDataStore.getFeatureSource(typeName);

            if (featureSource instanceof SimpleFeatureStore) {
                SimpleFeatureStore featureStore = (SimpleFeatureStore) featureSource;

                DefaultFeatureCollection collection = new DefaultFeatureCollection();
                collection.addAll(features);

                featureStore.setTransaction(transaction);
                try {
                    featureStore.addFeatures(collection);
                    transaction.commit();

                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    transaction.rollback();

                } finally {
                    transaction.close();
                }
            }

            return shpfile;
        } catch (Exception e) {
            logger.error("Error saving shape file", e);
            return null;
        } finally {
            if (newDataStore != null) {
                try {
                    newDataStore.dispose();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    private static SimpleFeatureType createFeatureType(String type) {

        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.setName("ActiveArea");
        builder.setCRS(DefaultGeographicCRS.WGS84); // <- Coordinate reference
        // system

        // add attributes in order
        if ("GEOMETRYCOLLECTION".equalsIgnoreCase(type)) {
            builder.add("the_geom", MultiPolygon.class);
        } else if ("MULTIPOLYGON".equalsIgnoreCase(type)) {
            builder.add("the_geom", MultiPolygon.class);
        } else {
            builder.add("the_geom", Polygon.class);
        }
        builder.length(50).add("name", String.class); // <- 50 chars width for
        // name field
        builder.length(100).add("desc", String.class); // 100 chars width
        // for description
        // field

        // build the type
        return builder.buildFeatureType();
    }

    static public String createCircleJs(double longitude, double latitude, double radius) {
        boolean belowMinus180 = false;
        double[][] points = new double[360][];
        for (int i = 0; i < 360; i++) {
            points[i] = computeOffset(latitude, 0, radius, i);
            if (points[i][0] + longitude < -180) {
                belowMinus180 = true;
            }
        }

        // longitude translation
        double dist = ((belowMinus180) ? 360 : 0) + longitude;

        StringBuilder s = new StringBuilder();
        s.append("POLYGON((");
        for (int i = 0; i < 360; i++) {
            s.append(points[i][0] + dist).append(" ").append(points[i][1]).append(",");
        }
        // append the first point to close the circle
        s.append(points[0][0] + dist).append(" ").append(points[0][1]);
        s.append("))");

        return s.toString();
    }

    private static double[] computeOffset(double lat, double lng, double radius, int angle) {
        double b = radius / 6378137.0;
        double c = angle * (Math.PI / 180.0);
        double e = lat * (Math.PI / 180.0);
        double d = Math.cos(b);
        b = Math.sin(b);
        double f = Math.sin(e);
        e = Math.cos(e);
        double g = d * f + b * e * Math.cos(c);

        double x = (lng * (Math.PI / 180.0) + Math.atan2(b * e * Math.sin(c), d - f * g)) / (Math.PI / 180.0);
        double y = Math.asin(g) / (Math.PI / 180.0);

        double[] pt = {x, y};

        return pt;
    }

    protected static class BaseFileNameInDirectoryFilter implements IOFileFilter {

        private String baseFileName;
        private File parentDir;
        private List<File> excludedFiles;

        public BaseFileNameInDirectoryFilter(String baseFileName, File parentDir, List<File> excludedFiles) {
            this.baseFileName = baseFileName;
            this.parentDir = parentDir;
            this.excludedFiles = new ArrayList<File>(excludedFiles);
        }

        @Override
        public boolean accept(File file) {
            if (excludedFiles.contains(file)) {
                return false;
            }
            return file.getParentFile().equals(parentDir) && file.getName().startsWith(baseFileName);
        }

        @Override
        public boolean accept(File dir, String name) {
            if (excludedFiles.contains(new File(dir, name))) {
                return false;
            }
            return dir.equals(parentDir) && name.startsWith(baseFileName);
        }

    }
}
