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
package au.org.ala.layers.intersect;

import au.org.ala.layers.dao.FieldDAO;
import au.org.ala.layers.dao.LayerDAO;
import au.org.ala.layers.dto.Field;
import au.org.ala.layers.dto.GridClass;
import au.org.ala.layers.dto.IntersectionFile;
import au.org.ala.layers.dto.Layer;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.*;
import java.util.Map.Entry;

/**
 * @author Adam
 */
public class IntersectConfig {

    private static final Logger LOGGER = Logger.getLogger(IntersectConfig.class);

    public static final String GEOSERVER_URL_PLACEHOLDER = "<COMMON_GEOSERVER_URL>";
    public static final String GEONETWORK_URL_PLACEHOLDER = "<COMMON_GEONETWORK_URL>";
    static final String ALASPATIAL_OUTPUT_PATH = "ALASPATIAL_OUTPUT_PATH";
    static final String LAYER_FILES_PATH = "LAYER_FILES_PATH";
    static final String ANALYSIS_LAYER_FILES_PATH = "ANALYSIS_LAYER_FILES_PATH";
    static final String ANALYSIS_TMP_LAYER_FILES_PATH = "ANALYSIS_TMP_LAYER_FILES_PATH";
    static final String LAYER_INDEX_URL = "LAYER_INDEX_URL";
    static final String BATCH_THREAD_COUNT = "BATCH_THREAD_COUNT";
    static final String CONFIG_RELOAD_WAIT = "CONFIG_RELOAD_WAIT";
    static final String PRELOADED_SHAPE_FILES = "PRELOADED_SHAPE_FILES";
    static final String GRID_BUFFER_SIZE = "GRID_BUFFER_SIZE";
    static final String GRID_CACHE_PATH = "GRID_CACHE_PATH";
    static final String GRID_CACHE_READER_COUNT = "GRID_CACHE_READER_COUNT";
    static final String LOCAL_SAMPLING = "LOCAL_SAMPLING";
    static final String GEOSERVER_URL = "GEOSERVER_URL";
    static final String GEONETWORK_URL = "GEONETWORK_URL";
    static final String GDAL_PATH = "GDAL_PATH";
    static final String ANALYSIS_RESOLUTIONS = "ANALYSIS_RESOLUTIONS";
    static final String OCCURRENCE_SPECIES_RECORDS_FILENAME = "OCCURRENCE_SPECIES_RECORDS_FILENAME";
    static final String UPLOADED_SHAPES_FIELD_ID = "UPLOADED_SHAPES_FIELD_ID";
    static final String API_KEY_CHECK_URL_TEMPLATE = "API_KEY_CHECK_URL_TEMPLATE";
    static final String SPATIAL_PORTAL_APP_NAME = "SPATIAL_PORTAL_APP_NAME";
    static final String BIOCACHE_SERVICE_URL = "BIOCACHE_SERVICE_URL";
    static final String GEOSERVER_USERNAME = "GEOSERVER_USERNAME";
    static final String GEOSERVER_PASSWORD = "GEOSERVER_PASSWORD";
    static final String SHP2PGSQL_PATH = "SHP2PGSQL_PATH";
    static final String GRIDS_TO_CACHE = "GRIDS_TO_CACHE";
    static final String CAN_UPDATE_LAYER_DISTANCES = "CAN_UPDATE_LAYER_DISTANCES";
    static final String CAN_UPDATE_GRID_CACHE = "CAN_UPDATE_GRID_CACHE";
    static final String CAN_GENERATE_ANALYSIS_FILES = "CAN_GENERATE_ANALYSIS_FILES";
    static final String CAN_INTERSECT_LAYERS = "CAN_INTERSECT_LAYERS";
    static final String CAN_GENRATE_THUMBNAILS = "CAN_GENRATE_THUMBNAILS";
    static final String LAYER_PROPERTIES = "layer.properties";
    /**
     * log4j logger
     */
    private static final Logger logger = Logger.getLogger(IntersectConfig.class);
    static ObjectMapper mapper = new ObjectMapper();
    static String layerFilesPath;
    static String analysisLayerFilesPath;
    static String analysisTmpLayerFilesPath;
    static String alaspatialOutputPath;
    static String layerIndexUrl;
    static int batchThreadCount;
    static long configReloadWait;
    static String preloadedShapeFiles;
    static int gridBufferSize;
    static String gridCachePath;
    static int gridCacheReaderCount;
    static boolean localSampling;
    static String geoserverUrl;
    static String geonetworkUrl;
    static String gdalPath;
    static List<Double> analysisResolutions;
    static String occurrenceSpeciesRecordsFilename;
    static String uploadedShapesFieldId;
    static String apiKeyCheckUrlTemplate;
    static String spatialPortalAppName;
    static String biocacheServiceUrl;
    static String geoserverUsername;
    static String geoserverPassword;
    static String shp2pgsqlPath;

    static {
        Properties properties = new Properties();
        InputStream is = null;
        try {
            String pth = "/data/layers-store/config/layers-store-config.properties";
            logger.debug("config path: " + pth);
            is = new FileInputStream(pth);
            if (is != null) {
                properties.load(is);
            } else {
                String msg = "cannot get properties file: " + pth;
                logger.warn(msg);
            }
        } catch (IOException ex) {
            logger.error(null, ex);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    logger.error("failed to close layers-store-config.properties", e);
                }
            }
        }

        layerFilesPath = getProperty(LAYER_FILES_PATH, properties, null);
        isValidPath(layerFilesPath, LAYER_FILES_PATH);
        analysisLayerFilesPath = getProperty(ANALYSIS_LAYER_FILES_PATH, properties, null);
        isValidPath(analysisLayerFilesPath, ANALYSIS_LAYER_FILES_PATH);
        analysisTmpLayerFilesPath = getProperty(ANALYSIS_TMP_LAYER_FILES_PATH, properties, null);
        isValidPath(analysisTmpLayerFilesPath, ANALYSIS_TMP_LAYER_FILES_PATH);
        alaspatialOutputPath = getProperty(ALASPATIAL_OUTPUT_PATH, properties, null);
        isValidPath(alaspatialOutputPath, ALASPATIAL_OUTPUT_PATH);
        layerIndexUrl = getProperty(LAYER_INDEX_URL, properties, null);
        isValidUrl(layerIndexUrl, LAYER_INDEX_URL);
        batchThreadCount = (int) getPositiveLongProperty(BATCH_THREAD_COUNT, properties, 1);
        configReloadWait = getPositiveLongProperty(CONFIG_RELOAD_WAIT, properties, 3600000);
        preloadedShapeFiles = getProperty(PRELOADED_SHAPE_FILES, properties, null);
        gridBufferSize = (int) getPositiveLongProperty(GRID_BUFFER_SIZE, properties, 4096);
        gridCachePath = getProperty(GRID_CACHE_PATH, properties, null);
        gridCacheReaderCount = (int) getPositiveLongProperty(GRID_CACHE_READER_COUNT, properties, 10);
        localSampling = getProperty(LOCAL_SAMPLING, properties, "true").toLowerCase().equals("true");
        geoserverUrl = getProperty(GEOSERVER_URL, properties, null);
        geoserverUsername = getProperty(GEOSERVER_USERNAME, properties, null);
        geoserverPassword = getProperty(GEOSERVER_PASSWORD, properties, null);

        geonetworkUrl = getProperty(GEONETWORK_URL, properties, null);

        gdalPath = getProperty(GDAL_PATH, properties, null);
        isValidPathGDAL(gdalPath, GDAL_PATH);
        analysisResolutions = getDoublesFrom(getProperty(ANALYSIS_RESOLUTIONS, properties, "0.5"));
        occurrenceSpeciesRecordsFilename = getProperty(OCCURRENCE_SPECIES_RECORDS_FILENAME, properties, null);
        uploadedShapesFieldId = getProperty(UPLOADED_SHAPES_FIELD_ID, properties, null);
        apiKeyCheckUrlTemplate = getProperty(API_KEY_CHECK_URL_TEMPLATE, properties, null);

        spatialPortalAppName = getProperty(SPATIAL_PORTAL_APP_NAME, properties, null);

        biocacheServiceUrl = getProperty(BIOCACHE_SERVICE_URL, properties, null);

        canGenerateAnalysisLayers = getProperty(CAN_GENERATE_ANALYSIS_FILES, properties, "false").equalsIgnoreCase("true");
        canGenerateThumbnails = getProperty(CAN_GENRATE_THUMBNAILS, properties, "false").equalsIgnoreCase("true");
        canIntersectLayers = getProperty(CAN_INTERSECT_LAYERS, properties, "false").equalsIgnoreCase("true");
        canUpdateGridCache = getProperty(CAN_UPDATE_GRID_CACHE, properties, "false").equalsIgnoreCase("true");
        canUpdateLayerDistances = getProperty(CAN_UPDATE_LAYER_DISTANCES, properties, "false").equalsIgnoreCase("true");

        String gridsToCache = getProperty(GRIDS_TO_CACHE, properties, "1");
        if ("all".equals(gridsToCache)) {
            Grid.maxGridsLoaded = -1;
        } else {
            try {
                Grid.maxGridsLoaded = Integer.parseInt(gridsToCache);
            } catch (Exception e) {
                LOGGER.error("failed to parse 'GRIDS_TO_CACHE' property as Integer: " + gridsToCache);
            }
        }

        shp2pgsqlPath = getProperty(SHP2PGSQL_PATH, properties, null);
    }

    long lastReload;
    SimpleShapeFileCache shapeFileCache;
    HashMap<String, IntersectionFile> intersectionFiles;
    HashMap<String, HashMap<Integer, GridClass>> classGrids;
    private FieldDAO fieldDao;
    private LayerDAO layerDao;
    static private boolean canUpdateLayerDistances;
    static private boolean canUpdateGridCache;
    static private boolean canGenerateAnalysisLayers;
    static private boolean canIntersectLayers;
    static private boolean canGenerateThumbnails;

    public IntersectConfig(FieldDAO fieldDao, LayerDAO layerDao) {
        this.fieldDao = fieldDao;
        this.layerDao = layerDao;

        load();
    }

    private static void isValidPath(String path, String desc) {
        File f = new File(path);

        if (!f.exists()) {
            logger.error("Config error. Property \"" + desc + "\" with value \"" + path + "\"  is not a valid local file path.  It does not exist.");
        } else if (!f.isDirectory()) {
            logger.error("Config error. Property \"" + desc + "\" with value \"" + path + "\"  is not a valid local file path.  It is not a directory.");
        } else if (!f.canRead()) {
            logger.error("Config error. Property \"" + desc + "\" with value \"" + path + "\"  is not a valid local file path.  Not permitted to READ.");
        } else if (!f.canWrite()) {
            logger.error("Config error. Property \"" + desc + "\" with value \"" + path + "\"  is not a valid local file path.  Not permitted to WRITE.");
        }

    }

    private static void isValidPathGDAL(String path, String desc) {
        File f = new File(path);

        if (!f.exists()) {
            logger.error("Config error. Property \"" + desc + "\" with value \"" + path + "\" is not a valid local file path.  It does not exist.");
        } else if (!f.isDirectory()) {
            logger.error("Config error. Property \"" + desc + "\" with value \"" + path + "\"  is not a valid local file path.  It is not a directory.");
        } else if (!f.canRead()) {
            logger.error("Config error. Property \"" + desc + "\" with value \"" + path + "\"  is not a valid local file path.  Not permitted to READ.");
        }

        //look for GDAL file "gdalwarp"
        File g = new File(path + File.separator + "gdalwarp");
        if (!f.exists()) {
            logger.error("Config error. Property \"" + desc + "\" with value \"" + path + "\"  is not a valid local file path.  gdalwarp does not exist.");
        } else if (!g.canExecute()) {
            logger.error("Config error. Property \"" + desc + "\" with value \"" + path + "\"  is not a valid local file path.  gdalwarp not permitted to EXECUTE.");
        }
    }

    private static void isValidUrl(String url, String desc) {
        HttpClient client = new HttpClient();
        GetMethod get = new GetMethod(url);

        try {
            int result = client.executeMethod(get);

            if (result != 200) {
                logger.error("Config error. Property \"" + desc + "\" with value \"" + url + "\"  is not a valid URL.  Error executing GET request, response=" + result);
            }
        } catch (Exception e) {
            logger.error("Config error. Property \"" + desc + "\" with value \"" + url + "\"  is not a valid URL.  Error executing GET request.");
        }

    }

    public static void setPreloadedShapeFiles(String preloadedShapeFiles) {
        IntersectConfig.preloadedShapeFiles = preloadedShapeFiles;
    }

    static String getProperty(String property, Properties properties, String defaultValue) {
        String p = System.getProperty(property);
        if (p == null) {
            p = properties.getProperty(property);
        }
        if (p == null) {
            p = defaultValue;
        }
        logger.info(property + " > " + p);
        return p;
    }

    static long getPositiveLongProperty(String property, Properties properties, long defaultValue) {
        String p = getProperty(property, properties, null);
        long l = defaultValue;
        try {
            l = Long.parseLong(p);
            if (l < 0) {
                l = defaultValue;
            }
        } catch (NumberFormatException ex) {
            logger.error("parsing " + property + ": " + p + ", using default: " + defaultValue, ex);
        }
        return l;
    }

    static public String getAlaspatialOutputPath() {
        return alaspatialOutputPath;
    }

    static public String getLayerFilesPath() {
        return layerFilesPath;
    }

    static public String getLayerIndexUrl() {
        return layerIndexUrl;
    }

    static public int getThreadCount() {
        return batchThreadCount;
    }

    static public int getGridBufferSize() {
        return gridBufferSize;
    }

    static public String getGridCachePath() {
        return gridCachePath;
    }

    static public int getGridCacheReaderCount() {
        return gridCacheReaderCount;
    }

    static private HashMap<Integer, GridClass> getGridClasses(String filePath, String type) throws IOException {
        HashMap<Integer, GridClass> classes = null;
        if (type.equals("Contextual")) {
            if (new File(filePath + ".gri").exists()
                    && new File(filePath + ".grd").exists()
                    && new File(filePath + ".txt").exists()) {
                File gridClassesFile = new File(filePath + ".classes.json");
                if (gridClassesFile.exists()) {
                    classes = mapper.readValue(gridClassesFile, new TypeReference<Map<Integer, GridClass>>() {
                    });
                    logger.info("found grid classes for " + gridClassesFile.getPath());
                } else {
                    logger.error("classes unavailable for " + gridClassesFile.getPath() + ", build classes offline");
                    //                logger.info("building " + gridClassesFile.getPath());
                    //                long start = System.currentTimeMillis();
                    //                classes = GridClassBuilder.buildFromGrid(filePath);
                    //                logger.info("finished building " + gridClassesFile.getPath() + " in " + (System.currentTimeMillis() - start) + " ms");
                }
            } else if (new File(filePath + ".gri").exists()
                    && new File(filePath + ".grd").exists()) {
                logger.error("missing grid classes for " + filePath);
            }
        } else {

        }
        return classes;
    }

    static public long getConfigReloadWait() {
        return configReloadWait;
    }

    static public String getGeoserverUrl() {
        return geoserverUrl;
    }

    static public String getGeonetworkUrl() {
        return geonetworkUrl;
    }

    static public String getAnalysisLayerFilesPath() {
        return analysisLayerFilesPath;
    }

    static public String getGdalPath() {
        return gdalPath;
    }

    static public List<Double> getAnalysisResolutions() {
        return analysisResolutions;
    }

    static private List<Double> getDoublesFrom(String property) {
        List<Double> l = new ArrayList<Double>();
        if (property != null) {
            for (String s : property.split(",")) {
                try {
                    Double d = Double.parseDouble(s.trim());
                    if (d != null && !d.isNaN()) {
                        l.add(d);
                    } else {
                        logger.warn("Cannot parse '" + s + "' to Double");
                    }
                } catch (Exception e) {
                    logger.warn("Cannot parse '" + s + "' to Double", e);
                }
            }
        }
        java.util.Collections.sort(l);
        return l;
    }

    static public String getOccurrenceSpeciesRecordsFilename() {
        return occurrenceSpeciesRecordsFilename;
    }

    static public String getUploadedShapesFieldId() {
        return uploadedShapesFieldId;
    }

    static public String getApiKeyCheckUrlTemplate() {
        return apiKeyCheckUrlTemplate;
    }

    static public String getSpatialPortalAppName() {
        return spatialPortalAppName;
    }

    public static String getAnalysisTmpLayerFilesPath() {
        return analysisTmpLayerFilesPath;
    }

    public String getBiocacheServiceUrl() {
        return biocacheServiceUrl;
    }

    public static void setMaxGridsLoaded(int maxGridsLoaded) {
        Grid.maxGridsLoaded = maxGridsLoaded;
    }

    public static int getMaxGridsLoaded() {
        return Grid.maxGridsLoaded;
    }

    public void load() {
        lastReload = System.currentTimeMillis();

        try {
            updateIntersectionFiles();
            updateShapeFileCache();

            System.out.println("**** grids to cache ***** = " + Grid.maxGridsLoaded);
            if (Grid.maxGridsLoaded <= 0) {
                seedGridFileCache();
            }
        } catch (Exception e) {
            //if it fails, set reload wait low
            logger.error("load failed, retry in 30s", e);
            configReloadWait = 30000;
        }
    }


    private void seedGridFileCache() {
        int count = 0;
        for (String s : intersectionFiles.keySet()) {
            if (s.startsWith("el") && intersectionFiles.get(s).getType().equalsIgnoreCase("environmental")) {
                try {
                    Grid g = Grid.getGrid(intersectionFiles.get(s).getFilePath());
                    g.getGrid();
                } catch (Exception e) {
                    System.out.println("error caching grid: " + s);
                }
                count++;
                if (count % 5 == 0) {
                    System.out.println("cached " + count + " grids");
                }
            }
        }
    }

    public IntersectionFile getIntersectionFile(String fieldId) {
        return intersectionFiles.get(fieldId);
    }

    public String getFieldIdFromFile(String file) {
        String off, on;
        if (File.separator.equals("/")) {
            off = "\\";
            on = "/";
        } else {
            on = "\\";
            off = "/";
        }
        file = file.replace(off, on);
        for (Entry<String, IntersectionFile> entry : intersectionFiles.entrySet()) {
            if (entry.getValue().getFilePath().replace(off, on).equalsIgnoreCase(file)) {
                return entry.getKey();
            }
        }
        return file;
    }

    public static void main(String[] args) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < 100000; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(Math.random() * (-12 + 44) - 44).append(',').append(Math.random() * (154 - 112) + 112);
        }
        try {
            FileUtils.writeStringToFile(new File("/data/p.txt"), sb.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void updateIntersectionFiles() throws MalformedURLException, IOException {
        if (intersectionFiles == null) {
            intersectionFiles = new HashMap<String, IntersectionFile>();
            classGrids = new HashMap<String, HashMap<Integer, GridClass>>();
        }

        if (layerIndexUrl != null) {
            //request from url
            JSONArray layers = JSONArray.fromObject(getUrl(layerIndexUrl + "/layers"));
            HashMap<String, String> layerPathOrig = new HashMap<String, String>();
            HashMap<String, String> layerName = new HashMap<String, String>();
            HashMap<String, String> layerType = new HashMap<String, String>();
            HashMap<String, String> layerPid = new HashMap<String, String>();
            for (int i = 0; i < layers.size(); i++) {
                layerPathOrig.put(layers.getJSONObject(i).getString("id"),
                        layers.getJSONObject(i).getString("path_orig"));
                layerName.put(layers.getJSONObject(i).getString("id"),
                        layers.getJSONObject(i).getString("name"));
                layerType.put(layers.getJSONObject(i).getString("id"),
                        layers.getJSONObject(i).getString("type"));
                layerPid.put(layers.getJSONObject(i).getString("id"),
                        layers.getJSONObject(i).getString("id"));
            }

            JSONArray fields = JSONArray.fromObject(getUrl(layerIndexUrl + "/fields"));
            for (int i = 0; i < fields.size(); i++) {
                JSONObject jo = fields.getJSONObject(i);
                String spid = jo.getString("spid");
                if (layerPathOrig.get(spid) == null) {
                    logger.error("cannot find layer with id '" + spid + "'");
                    continue;
                }
                HashMap<Integer, GridClass> gridClasses =
                        getGridClasses(layerFilesPath + layerPathOrig.get(spid), layerType.get(spid));

                IntersectionFile intersectionFile = new IntersectionFile(jo.getString("name"),
                        layerFilesPath + layerPathOrig.get(spid),
                        (jo.containsKey("sname") ? jo.getString("sname") : null),
                        layerName.get(jo.getString("spid")),
                        jo.getString("id"),
                        jo.getString("name"),
                        layerPid.get(spid),
                        jo.getString("type"),
                        gridClasses);

                intersectionFiles.put(jo.getString("id"), intersectionFile);
                //also register it under the layer name
                intersectionFiles.put(layerName.get(spid), intersectionFile);
                //also register it under the layer pid
                intersectionFiles.put(layerPid.get(spid), intersectionFile);
                classGrids.put(jo.getString("id"), gridClasses);
            }
        } else {
            for (Field f : fieldDao.getFields()) {
                if (f.isEnabled()) {
                    Layer layer = layerDao.getLayerById(Integer.parseInt(f.getSpid()), false);
                    if (layer == null) {
                        logger.error("cannot find layer with id '" + f.getSpid() + "'");
                        continue;
                    }
                    HashMap<Integer, GridClass> gridClasses = getGridClasses(getLayerFilesPath() + layer.getPath_orig(), layer.getType());
                    IntersectionFile intersectionFile = new IntersectionFile(f.getName(),
                            getLayerFilesPath() + layer.getPath_orig(),
                            f.getSname(),
                            layer.getName(),
                            f.getId(),
                            f.getName(),
                            String.valueOf(layer.getId()),
                            f.getType(),
                            gridClasses);

                    intersectionFiles.put(f.getId(), intersectionFile);
                    //also register it under the layer name
                    //- only if default layer not already added
                    if (f.isDefaultlayer() || intersectionFiles.get(layer.getName()) == null) {
                        intersectionFiles.put(layer.getName(), intersectionFile);
                    }
                    //also register it under the layer pid
                    if (f.isDefaultlayer() || intersectionFiles.get(String.valueOf(layer.getId())) == null) {
                        intersectionFiles.put(String.valueOf(layer.getId()), intersectionFile);
                    }
                    classGrids.put(f.getId(), gridClasses);
                }
            }
        }
    }

    String getUrl(String url) {
        try {
            logger.info("opening url: " + url);

            HttpClient client = new HttpClient();
            GetMethod get = new GetMethod(url);

            int result = client.executeMethod(get);
            String slist = get.getResponseBodyAsString();
            return slist;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    public void updateShapeFileCache() {
        if (preloadedShapeFiles == null || preloadedShapeFiles.length() == 0) {
            return;
        }

        String[] fields = preloadedShapeFiles.split(",");

        //requres readLayerInfo() first
        String[] layers = new String[fields.length];
        String[] columns = new String[fields.length];
        String[] fid = new String[fields.length];
        if (fields.length == 1 && fields[0].equalsIgnoreCase("all")) {
            Set shapefiles = new HashSet();
            for (String s : intersectionFiles.keySet()) {
                if (s.startsWith("cl") && intersectionFiles.get(s).getType().equalsIgnoreCase("contextual")) {
                    shapefiles.add(intersectionFiles.get(s).getFilePath());
                }
            }
            layers = new String[shapefiles.size()];
            columns = new String[shapefiles.size()];
            fid = new String[shapefiles.size()];
            int i = 0;
            for (String s : intersectionFiles.keySet()) {
                if (shapefiles.contains(intersectionFiles.get(s).getFilePath())) {
                    shapefiles.remove(intersectionFiles.get(s).getFilePath());

                    IntersectionFile f = intersectionFiles.get(s);
                    layers[i] = f.getFilePath();
                    columns[i] = f.getShapeFields();
                    fid[i] = f.getFieldId();
                    i++;
                }
            }
        } else {
            for (int i = 0; i < fields.length; i++) {
                try {
                    layers[i] = getIntersectionFile(fields[i].trim()).getFilePath();
                    columns[i] = getIntersectionFile(fields[i].trim()).getShapeFields();
                    fid[i] = fields[i];
                } catch (Exception e) {
                    logger.error("failed to load shapefile for field: " + fields[i], e);
                }
            }
        }

        if (shapeFileCache == null) {
            shapeFileCache = new SimpleShapeFileCache(layers, columns, fid);
        } else {
            shapeFileCache.update(layers, columns, fid);
        }
    }

    /**
     * Add shape files to the shape file cache.
     *
     * @param fieldIds comma separated fieldIds.  Must be cl fields.
     */
    public void addToShapeFileCache(String fieldIds) {
        if (preloadedShapeFiles != null) {
            fieldIds += "," + preloadedShapeFiles;
        }
        String[] fields = fieldIds.split(",");

        //requres readLayerInfo() first
        String[] layers = new String[fields.length];
        String[] columns = new String[fields.length];
        String[] fid = new String[fields.length];

        int pos = 0;
        for (int i = 0; i < fields.length; i++) {
            try {
                layers[pos] = getIntersectionFile(fields[i].trim()).getFilePath();
                columns[pos] = getIntersectionFile(fields[i].trim()).getShapeFields();
                fid[pos] = fields[i];
                pos++;
            } catch (Exception e) {
                logger.error("problem adding shape file to cache for field: " + fields[i], e);
            }
        }
        if (pos < layers.length) {
            layers = java.util.Arrays.copyOf(layers, pos);
            columns = java.util.Arrays.copyOf(columns, pos);
            fid = java.util.Arrays.copyOf(fid, pos);
        }

        if (shapeFileCache == null) {
            shapeFileCache = new SimpleShapeFileCache(layers, columns, fid);
        } else {
            shapeFileCache.update(layers, columns, fid);
        }
    }

    public SimpleShapeFileCache getShapeFileCache() {
        return shapeFileCache;
    }

    public boolean requiresReload() {
        return lastReload + configReloadWait >= System.currentTimeMillis();
    }

    public boolean isLocalSampling() {
        return localSampling;
    }

    public List<Field> getFieldsByDB() {
        List<Field> fields = new ArrayList<Field>();
        if (layerIndexUrl != null) {
            try {
                //request from url
                fields = mapper.readValue(getUrl(layerIndexUrl + "/fieldsdb"), new TypeReference<List<Field>>() {
                });
            } catch (Exception ex) {
                logger.error("failed to read: " + layerIndexUrl + "/fieldsdb", ex);
            }
        }
        return fields;
    }

    public Map<String, IntersectionFile> getIntersectionFiles() {
        return intersectionFiles;
    }

    /**
     * get info on an analysis layer
     *
     * @param id layer id as String
     * @return String [] with [0] = analysis id, [1] = path to grid file, [2] = analysis type
     */
    public String[] getAnalysisLayerInfo(String id) {
        String gid, filename, name;
        gid = filename = name = null;
        if (id.startsWith("species_")) {
            //maxent layer
            gid = id.substring("species_".length());
            filename = getAlaspatialOutputPath() + File.separator + "maxent" + File.separator + gid + File.separator + gid;
            name = "Prediction";
        } else if (id.startsWith("aloc_")) {
            //aloc layer
            gid = id.substring("aloc_".length());
            filename = getAlaspatialOutputPath() + File.separator + "aloc" + File.separator + gid + File.separator + "aloc";
            name = "Classification";
        } else if (id.startsWith("odensity_")) {
            //occurrence density layer
            gid = id.substring("odensity_".length());
            filename = getAlaspatialOutputPath() + File.separator + "sitesbyspecies" + File.separator + gid + File.separator + "occurrence_density";
            name = "Occurrence Density";
        } else if (id.startsWith("srichness_")) {
            //species richness layer
            gid = id.substring("srichness_".length());
            filename = getAlaspatialOutputPath() + File.separator + "sitesbyspecies" + File.separator + gid + File.separator + "species_richness";
            name = "Species Richness";
        } else if (id.endsWith("_odensity")) {
            //occurrence density layer
            gid = id.substring(0, id.length() - "_odensity".length());
            filename = getAlaspatialOutputPath() + File.separator + "sitesbyspecies" + File.separator + gid + File.separator + "occurrence_density";
            name = "Occurrence Density";
        } else if (id.endsWith("_srichness")) {
            //species richness layer
            gid = id.substring(0, id.length() - "_srichness".length());
            filename = getAlaspatialOutputPath() + File.separator + "sitesbyspecies" + File.separator + gid + File.separator + "species_richness";
            name = "Species Richness";
        } else if (id.startsWith("envelope_")) {
            //envelope layer
            gid = id.substring("envelope_".length());
            filename = getAlaspatialOutputPath() + File.separator + "envelope" + File.separator + gid + File.separator + "envelope";
            name = "Environmental Envelope";
        } else if (id.startsWith("gdm_")) {
            //gdm layer
            int pos1 = id.indexOf("_");
            int pos2 = id.lastIndexOf("_");
            String[] gdmparts = new String[]{id.substring(0, pos1), id.substring(pos1 + 1, pos2), id.substring(pos2 + 1)};
            gid = gdmparts[2];
            filename = getAlaspatialOutputPath() + File.separator + "gdm" + File.separator + gid + File.separator + gdmparts[1];
            //Layer tmpLayer = layerDao.getLayerByName(gdmparts[1].replaceAll("Tran", ""));
            //name = "Transformed " + tmpLayer.getDisplayname();
            name = "Transformed " + getIntersectionFile(gdmparts[1].replaceAll("Tran", "")).getFieldName();
        } else if (id.contains("_")) {
            //2nd form of gdm layer name, why?
            int pos = id.indexOf("_");
            String[] gdmparts = new String[]{id.substring(0, pos), id.substring(pos + 1)};
            gid = gdmparts[0];
            filename = getAlaspatialOutputPath() + File.separator + "gdm" + File.separator + gid + File.separator + gdmparts[1] + "Tran";
            logger.debug("id: " + id);
            logger.debug("parts: " + gdmparts[0] + ", " + gdmparts[1]);
            logger.debug("filename: " + filename);
            //Layer tmpLayer = layerDao.getLayerByName(gdmparts[1].replaceAll("Tran", ""));
            //name = "Transformed " + tmpLayer.getDisplayname();
            name = "Transformed " + getIntersectionFile(gdmparts[1]).getFieldName();
        }

        if (gid != null) {
            return new String[]{gid, filename, name};
        } else {
            return null;
        }
    }

    public String getGeoserverUsername() {
        return geoserverUsername;
    }

    public String getGeoserverPassword() {
        return geoserverPassword;
    }

    public String getShp2pgsqlPath() {
        return shp2pgsqlPath;
    }

    static public boolean getCanUpdateLayerDistances() {
        return canUpdateLayerDistances;
    }

    static public boolean getCanUpdateGridCache() {
        return canUpdateGridCache;
    }

    static public boolean getCanGenerateAnalysisLayers() {
        return canGenerateAnalysisLayers;
    }

    static public boolean getCanIntersectLayers() {
        return canIntersectLayers;
    }


    static public boolean getCanGenerateThumbnails() {
        return canGenerateThumbnails;
    }
}
