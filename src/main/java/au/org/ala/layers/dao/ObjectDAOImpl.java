/**************************************************************************
 * Copyright (C) 2010 Atlas of Living Australia
 * All Rights Reserved.
 * <p>
 * The contents of this file are subject to the Mozilla Public
 * License Version 1.1 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at http://www.mozilla.org/MPL/
 * <p>
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 ***************************************************************************/
package au.org.ala.layers.dao;

import au.org.ala.layers.dto.GridClass;
import au.org.ala.layers.dto.IntersectionFile;
import au.org.ala.layers.dto.Objects;
import au.org.ala.layers.intersect.Grid;
import au.org.ala.layers.intersect.IntersectConfig;
import au.org.ala.layers.util.LayerFilter;
import au.org.ala.layers.util.SpatialConversionUtils;
import au.org.ala.layers.util.SpatialUtil;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.geotools.data.DataUtilities;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.kml.KML;
import org.geotools.kml.KMLConfiguration;
import org.geotools.xsd.Encoder;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import org.opengis.feature.simple.SimpleFeatureType;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.io.*;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.zip.ZipInputStream;

/**
 * @author ajay
 */
@Service("objectDao")
public class ObjectDAOImpl implements ObjectDAO, ApplicationContextAware {

    static final String objectWmsUrl = "/wms?service=WMS&version=1.1.0&request=GetMap&layers=ALA:Objects&format=image/png&viewparams=s:<pid>";
    static final String gridPolygonSld;
    static final String gridClassSld;

    // sld substitution strings
    private static final String SUB_LAYERNAME = "*layername*";
    static final String gridPolygonWmsUrl = "/wms?service=WMS&version=1.1.0&request=GetMap&layers=ALA:" + SUB_LAYERNAME + "&format=image/png&sld_body=";
    private static final String SUB_COLOUR = "0xff0000"; // "*colour*";
    private static final String SUB_MIN_MINUS_ONE = "*min_minus_one*";
    private static final String SUB_MIN = "*min*";
    private static final String SUB_MAX = "*max*";
    private static final String SUB_MAX_PLUS_ONE = "*max_plus_one*";
    private static final String KML_HEADER =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                    + "<kml xmlns=\"http://earth.google.com/kml/2.2\">"
                    + "<Document>"
                    + "  <name></name>"
                    + "  <description></description>"
                    + "  <Style id=\"style1\">"
                    + "    <LineStyle>"
                    + "      <color>40000000</color>"
                    + "      <width>3</width>"
                    + "    </LineStyle>"
                    + "    <PolyStyle>"
                    + "      <color>73FF0000</color>"
                    + "      <fill>1</fill>"
                    + "      <outline>1</outline>"
                    + "    </PolyStyle>"
                    + "  </Style>"
                    + "  <Placemark>"
                    + "    <name></name>"
                    + "    <description></description>"
                    + "    <styleUrl>#style1</styleUrl>";
    private static final String KML_FOOTER =
            "</Placemark>"
                    + "</Document>"
                    + "</kml>";
    /**
     * log4j logger
     */
    private static final Logger logger = Logger.getLogger(ObjectDAOImpl.class);

    static {
        String polygonSld = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><StyledLayerDescriptor xmlns=\"http://www.opengis.net/sld\">" + "<NamedLayer><Name>ALA:" + SUB_LAYERNAME + "</Name>"
                + "<UserStyle><FeatureTypeStyle><Rule><RasterSymbolizer><Geometry></Geometry>" + "<ColorMap>" + "<ColorMapEntry color=\"" + SUB_COLOUR + "\" opacity=\"0\" quantity=\""
                + SUB_MIN_MINUS_ONE + "\"/>" + "<ColorMapEntry color=\"" + SUB_COLOUR + "\" opacity=\"1\" quantity=\"" + SUB_MIN + "\"/>" + "<ColorMapEntry color=\"" + SUB_COLOUR
                + "\" opacity=\"0\" quantity=\"" + SUB_MAX_PLUS_ONE + "\"/>" + "</ColorMap></RasterSymbolizer></Rule></FeatureTypeStyle></UserStyle></NamedLayer></StyledLayerDescriptor>";

        String classSld = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><StyledLayerDescriptor xmlns=\"http://www.opengis.net/sld\">" + "<NamedLayer><Name>ALA:" + SUB_LAYERNAME + "</Name>"
                + "<UserStyle><FeatureTypeStyle><Rule><RasterSymbolizer><Geometry></Geometry>" + "<ColorMap>" + "<ColorMapEntry color=\"" + SUB_COLOUR + "\" opacity=\"0\" quantity=\""
                + SUB_MIN_MINUS_ONE + "\"/>" + "<ColorMapEntry color=\"" + SUB_COLOUR + "\" opacity=\"1\" quantity=\"" + SUB_MIN + "\"/>" + "<ColorMapEntry color=\"" + SUB_COLOUR
                + "\" opacity=\"1\" quantity=\"" + SUB_MAX + "\"/>" + "<ColorMapEntry color=\"" + SUB_COLOUR + "\" opacity=\"0\" quantity=\"" + SUB_MAX_PLUS_ONE + "\"/>"
                + "</ColorMap></RasterSymbolizer></Rule></FeatureTypeStyle></UserStyle></NamedLayer></StyledLayerDescriptor>";
        try {
            polygonSld = URLEncoder.encode(polygonSld, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            logger.fatal("Invalid polygon sld string defined in ObjectDAOImpl.");
        }
        try {
            classSld = URLEncoder.encode(classSld, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            logger.fatal("Invalid class sld string defined in ObjectDAOImpl.");
        }

        gridPolygonSld = polygonSld;
        gridClassSld = classSld;

    }

    private JdbcTemplate jdbcTemplate;
    @Resource(name = "layerIntersectDao")
    private LayerIntersectDAO layerIntersectDao;
    private ApplicationContext applicationContext;

    @Resource(name = "dataSource")
    public void setDataSource(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public List<Objects> getObjects() {
        logger.info("Getting a list of all objects");
        String sql = "select o.pid as pid, o.id as id, o.name as name, o.desc as description, o.fid as fid, " +
                "f.name as fieldname, o.area_km as area_km from objects o, fields f where o.fid = f.id";
        List<Objects> objects = jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Objects.class));
        updateObjectWms(objects);
        return objects;
    }

    @Override
    public List<Objects> getObjectsById(String id) {
        return getObjectsById(id, 0, -1);
    }

    public void writeObjectsToCSV(OutputStream output, String fid) throws Exception {
        String sql = MessageFormat.format("COPY (select o.pid as pid, o.id as id, o.name as name, " +
                "o.desc as description, " +
                "ST_AsText(ST_Centroid(o.the_geom)) as centroid, " +
                "GeometryType(o.the_geom) as featureType from objects o " +
                "where o.fid = ''{0}'') TO STDOUT WITH CSV HEADER", fid);

        DataSource ds = (DataSource) applicationContext.getBean("dataSource");
        Connection conn = DataSourceUtils.getConnection(ds);

        try {
            BaseConnection baseConn= conn.unwrap(BaseConnection.class);
            Writer csvOutput = new OutputStreamWriter(output);
            CopyManager copyManager = new CopyManager(baseConn);
            copyManager.copyOut(sql, csvOutput);
            csvOutput.flush();
            conn.close();
        } catch (SQLException ex) {
            // something has failed and we print a stack trace to analyse the error
            logger.error(ex.getMessage(), ex);
            // ignore failure closing connection
            try {
                conn.close();
            } catch (SQLException e) { /*do nothing for failure to close */ }
        } finally {
            // properly release our connection
            DataSourceUtils.releaseConnection(conn, ds);
        }
    }

    @Override
    public List<Objects> getObjectsById(String id, int start, int pageSize) {
        return getObjectsById(id, start, pageSize, null);
    }

    @Override
    public List<Objects> getObjectsById(String id, int start, int pageSize, String filter) {
        if (filter == null) filter = "";
        String upperCaseFilter = filter.toUpperCase();
        filter = "%" + filter + "%";

        logger.info("Getting object info for fid = " + id);
        String limit_offset = " limit " + (pageSize < 0 ? "all" : pageSize) + " offset " + start;
        String sql = "select o.pid as pid, o.id as id, o.name as name, o.desc as description, " +
                "o.fid as fid, f.name as fieldname, o.bbox, o.area_km, " +
                "ST_AsText(ST_Centroid(o.the_geom)) as centroid," +
                "GeometryType(o.the_geom) as featureType from objects o, fields f " +
                "where o.fid = ? and o.fid = f.id and (o.name ilike ? or o.desc ilike ? ) order by o.pid " + limit_offset;
        List<Objects> objects = jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Objects.class), id, filter, filter);

        updateObjectWms(objects);

        // get grid classes
        if (objects == null || objects.isEmpty()) {
            objects = new ArrayList<Objects>();
            IntersectionFile f = layerIntersectDao.getConfig().getIntersectionFile(id);
            if (f != null && f.getClasses() != null) {
                //shape position
                int pos = 0;

                for (Entry<Integer, GridClass> c : f.getClasses().entrySet()) {
                    File file = new File(f.getFilePath() + File.separator + c.getKey() + ".wkt.index.dat");
                    if ((f.getType().equals("a") || !file.exists()) &&
                            c.getValue().getName().toUpperCase().contains(upperCaseFilter)) { // class pid
                        if (pageSize == -1 || (pos >= start && pos - start < pageSize)) {
                            Objects o = new Objects();
                            o.setPid(f.getLayerPid() + ":" + c.getKey());
                            o.setId(f.getLayerPid() + ":" + c.getKey());
                            o.setName(c.getValue().getName());
                            o.setFid(f.getFieldId());
                            o.setFieldname(f.getFieldName());
                            o.setBbox(c.getValue().getBbox());
                            o.setArea_km(c.getValue().getArea_km());
                            o.setWmsurl(getGridClassWms(f.getLayerName(), c.getValue()));
                            objects.add(o);
                        }
                        pos++;

                        if (pageSize != -1 && pos >= start + pageSize) {
                            break;
                        }
                    } else { // polygon pid
                        RandomAccessFile raf = null;
                        try {
                            raf = new RandomAccessFile(file, "r");
                            long itemSize = (4 + 4 + 4 * 4 + 4);
                            long len = raf.length() / itemSize; // group

                            if (pageSize != -1 && pos + len < start) {
                                pos += len;
                            } else {
                                // number,
                                // character
                                // offset,
                                // minx,
                                // miny,
                                // maxx,
                                // maxy,
                                // area
                                // sq
                                // km
                                int i = 0;
                                if (pageSize != -1 && pos < start) {
                                    //the first object requested is in this file, seek to the start
                                    i = start - pos;
                                    pos += i;
                                    raf.seek(i * itemSize);
                                }
                                for (; i < len; i++) {
                                    int n = raf.readInt();
                                    /* int charoffset = */
                                    raf.readInt();
                                    float minx = raf.readFloat();
                                    float miny = raf.readFloat();
                                    float maxx = raf.readFloat();
                                    float maxy = raf.readFloat();
                                    float area = raf.readFloat();

                                    if (pageSize == -1 || (pos >= start && pos - start < pageSize)) {
                                        Objects o = new Objects();
                                        o.setPid(f.getLayerPid() + ":" + c.getKey() + ":" + n);
                                        o.setId(f.getLayerPid() + ":" + c.getKey() + ":" + n);
                                        o.setName(c.getValue().getName());
                                        o.setFid(f.getFieldId());
                                        o.setFieldname(f.getFieldName());

                                        o.setBbox("POLYGON((" + minx + " " + miny + "," + minx + " " + maxy + "," + +maxx + " " + maxy + "," + +maxx + " " + miny + "," + +minx + " " + miny + "))");
                                        o.setArea_km(1.0 * area);

                                        o.setWmsurl(getGridPolygonWms(f.getLayerName(), n));

                                        objects.add(o);
                                    }

                                    pos++;

                                    if (pageSize != -1 && pos >= start + pageSize) {
                                        break;
                                    }
                                }
                            }
                        } catch (Exception e) {
                            logger.error(e.getMessage(), e);
                        } finally {
                            if (raf != null) {
                                try {
                                    raf.close();
                                } catch (Exception e) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                        }

                        if (pageSize != -1 && pos >= start + pageSize) {
                            break;
                        }
                    }
                }
            }
        }
        return objects;
    }

    @Override
    public String getObjectsGeometryById(String id, String geomtype) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            streamObjectsGeometryById(baos, id, geomtype);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                baos.close();
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }

        return new String(baos.toByteArray());
    }

    @Override
    public void streamObjectsGeometryById(OutputStream os, String id, String geomtype) throws IOException {
        logger.info("Getting object info for id = " + id + " and geometry as " + geomtype);
        String sql = "";
        if ("kml".equals(geomtype)) {
            sql = "SELECT ST_AsKml(the_geom,15) as geometry, name, \"desc\" as description  FROM objects WHERE pid=?;";
        } else if ("wkt".equals(geomtype)) {
            sql = "SELECT ST_AsText(the_geom,15) as geometry FROM objects WHERE pid=?;";
        } else if ("geojson".equals(geomtype)) {
            sql = "SELECT ST_AsGeoJSON(the_geom,15) as geometry FROM objects WHERE pid=?;";
        } else if ("shp".equals(geomtype)) {
            sql = "SELECT ST_AsText(the_geom,15) as geometry, name, \"desc\" as description FROM objects WHERE pid=?;";
        }

        List<Objects> l = jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Objects.class), id);

        if (l.size() > 0) {
            if ("shp".equals(geomtype)) {
                String wkt = l.get(0).getGeometry();
                File zippedShapeFile = SpatialConversionUtils.buildZippedShapeFile(wkt, id, l.get(0).getName(), l.get(0).getDescription());
                FileUtils.copyFile(zippedShapeFile, os);
            } else if ("kml".equals(geomtype)) {
                os.write(KML_HEADER
                        .replace("<name></name>", "<name><![CDATA[" + l.get(0).getName() + "]]></name>")
                        .replace("<description></description>", "<description><![CDATA[" + l.get(0).getDescription() + "]]></description>").getBytes());

                os.write(l.get(0).getGeometry().getBytes());
                os.write(KML_FOOTER.getBytes());
            } else {
                os.write(l.get(0).getGeometry().getBytes());
            }

        } else {
            // get grid classes
            if (id.length() > 0) {
                // grid class pids are, 'layerPid:gridClassNumber'
                try {
                    String[] s = id.split(":");
                    if (s.length >= 2) {
                        int n = Integer.parseInt(s[1]);
                        IntersectionFile f = layerIntersectDao.getConfig().getIntersectionFile(s[0]);
                        if (f != null && f.getClasses() != null) {
                            GridClass gc = f.getClasses().get(n);
                            if (gc != null && ("kml".equals(geomtype) || "wkt".equals(geomtype) || "geojson".equals(geomtype) || "shp".equals(geomtype))) {
                                // TODO: enable for type 'a' after
                                // implementation of fields table defaultLayer
                                // field

                                File file = new File(f.getFilePath() + File.separator + s[1] + "." + geomtype + ".zip");
                                if ((f.getType().equals("a") || s.length == 2) && file.exists()) {
                                    ZipInputStream zis = null;
                                    try {
                                        zis = new ZipInputStream(new FileInputStream(file));

                                        zis.getNextEntry();
                                        byte[] buffer = new byte[1024];
                                        int size;
                                        while ((size = zis.read(buffer)) > 0) {
                                            os.write(buffer, 0, size);
                                        }
                                    } catch (Exception e) {
                                        logger.error(e.getMessage(), e);
                                    } finally {
                                        if (zis != null) {
                                            try {
                                                zis.close();
                                            } catch (Exception e) {
                                                logger.error(e.getMessage(), e);
                                            }
                                        }
                                    }
                                } else { // polygon
                                    BufferedInputStream bis = null;
                                    InputStreamReader isr = null;
                                    try {
                                        String[] cells = null;

                                        HashMap<String, Object> map = s.length == 2 ? null : getGridIndexEntry(f.getFilePath() + File.separator + s[1], s[2]);

                                        String wkt = null;
                                        if (map != null) {
                                            cells = new String[]{s[2], String.valueOf(map.get("charoffset"))};
                                            if (cells != null) {
                                                // get polygon wkt string
                                                File file2 = new File(f.getFilePath() + File.separator + s[1] + ".wkt");
                                                bis = new BufferedInputStream(new FileInputStream(file2));
                                                isr = new InputStreamReader(bis);
                                                isr.skip(Long.parseLong(cells[1]));
                                                char[] buffer = new char[1024];
                                                int size;
                                                StringBuilder sb = new StringBuilder();
                                                sb.append("POLYGON");
                                                int end = -1;
                                                while (end < 0 && (size = isr.read(buffer)) > 0) {
                                                    sb.append(buffer, 0, size);
                                                    end = sb.toString().indexOf("))");
                                                }
                                                end += 2;

                                                wkt = sb.toString().substring(0, end);
                                            }
                                        } else {
                                            wkt = gc.getBbox();
                                        }

                                        if (geomtype.equals("wkt")) {
                                            os.write(wkt.getBytes());
                                        } else {
                                            WKTReader r = new WKTReader();
                                            Geometry g = r.read(wkt);

                                            if (geomtype.equals("kml")) {
                                                os.write(KML_HEADER.getBytes());
                                                Encoder encoder = new Encoder(new KMLConfiguration());
                                                encoder.setIndenting(true);
                                                encoder.encode(g, KML.Geometry, os);
                                                os.write(KML_FOOTER.getBytes());
                                            } else if (geomtype.equals("geojson")) {
                                                FeatureJSON fjson = new FeatureJSON();
                                                final SimpleFeatureType TYPE = DataUtilities.createType("class", "the_geom:MultiPolygon,name:String");
                                                SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(TYPE);
                                                featureBuilder.add(g);
                                                featureBuilder.add(gc.getName());
                                                fjson.writeFeature(featureBuilder.buildFeature(null), os);
                                            } else if (geomtype == "shp") {
                                                File zippedShapeFile = SpatialConversionUtils.buildZippedShapeFile(wkt, id, gc.getName(), null);
                                                FileUtils.copyFile(zippedShapeFile, os);
                                            }
                                        }
                                    } catch (Exception e) {
                                        logger.error(e.getMessage(), e);
                                    } finally {
                                        if (bis != null) {
                                            try {
                                                bis.close();
                                            } catch (Exception e) {
                                                logger.error(e.getMessage(), e);
                                            }
                                        }
                                        if (isr != null) {
                                            try {
                                                isr.close();
                                            } catch (Exception e) {
                                                logger.error(e.getMessage(), e);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    @Override
    public Objects getObjectByPid(String pid) {
        logger.info("Getting object info for pid = " + pid);
        String sql = "select o.pid, o.id, o.name, o.desc as description, o.fid as fid, f.name as fieldname, " +
                "o.bbox, o.area_km from objects o, fields f where o.pid = ? and o.fid = f.id";
        List<Objects> l = jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Objects.class), pid);

        updateObjectWms(l);

        // get grid classes
        if ((l == null || l.isEmpty()) && pid.length() > 0) {
            // grid class pids are, 'layerPid:gridClassNumber'
            try {
                String[] s = pid.split(":");
                if (s.length >= 2) {
                    int n = Integer.parseInt(s[1]);
                    IntersectionFile f = layerIntersectDao.getConfig().getIntersectionFile(s[0]);
                    if (f != null && f.getClasses() != null) {
                        GridClass gc = f.getClasses().get(n);
                        if (gc != null) {
                            Objects o = new Objects();
                            o.setPid(pid);
                            o.setId(pid);
                            o.setName(gc.getName());
                            o.setFid(f.getFieldId());
                            o.setFieldname(f.getFieldName());

                            if (f.getType().equals("a") || s.length == 2) {
                                o.setBbox(gc.getBbox());
                                o.setArea_km(gc.getArea_km());
                                o.setWmsurl(getGridClassWms(f.getLayerName(), gc));
                            } else {
                                HashMap<String, Object> map = getGridIndexEntry(f.getFilePath() + File.separator + s[1], s[2]);
                                if (!map.isEmpty()) {
                                    o.setBbox("POLYGON(" + map.get("minx") + " " + map.get("miny") + "," + map.get("minx") + " " + map.get("maxy") + "," + map.get("maxx") + " " + map.get("maxy")
                                            + "," + map.get("maxx") + " " + map.get("miny") + "," + map.get("minx") + " " + map.get("miny") + ")");

                                    o.setArea_km(((Float) map.get("area")).doubleValue());

                                    o.setWmsurl(getGridPolygonWms(f.getLayerName(), Integer.parseInt(s[2])));
                                }
                            }

                            l.add(o);
                        }
                    }
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        if (l.size() > 0) {
            return l.get(0);
        } else {
            return null;
        }
    }

    @Override
    public Objects getObjectByIdAndLocation(String fid, Double lng, Double lat) {
        logger.info("Getting object info for fid = " + fid + " at loc: (" + lng + ", " + lat + ") ");
        String sql = "select o.pid, o.id, o.name, o.desc as description, o.fid as fid, f.name as fieldname, " +
                "o.bbox, o.area_km from search_objects_by_geometry_intersect(?, " +
                "ST_GeomFromText('POINT(" + lng + " " + lat + ")', 4326)) o, fields f WHERE o.fid = f.id";
        List<Objects> l = jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Objects.class), fid);
        updateObjectWms(l);
        if (l == null || l.isEmpty()) {
            // get grid classes intersection
            l = new ArrayList<Objects>();
            IntersectionFile f = layerIntersectDao.getConfig().getIntersectionFile(fid);
            if (f != null && f.getClasses() != null) {
                Vector v = layerIntersectDao.samplingFull(fid, lng, lat);
                if (v != null && v.size() > 0 && v.get(0) != null) {
                    Map m = (Map) v.get(0);
                    int key = (int) Double.parseDouble(((String) m.get("pid")).split(":")[1]);
                    GridClass gc = f.getClasses().get(key);
                    if (f.getType().equals("a") || !new File(f.getFilePath() + File.separator + "polygons.grd").exists()) { // class pid
                        Objects o = new Objects();
                        o.setName(gc.getName());
                        o.setFid(f.getFieldId());
                        o.setFieldname(f.getFieldName());
                        o.setPid(f.getLayerPid() + ":" + gc.getId());
                        o.setId(f.getLayerPid() + ":" + gc.getId());
                        o.setBbox(gc.getBbox());
                        o.setArea_km(gc.getArea_km());
                        o.setWmsurl(getGridClassWms(f.getLayerName(), gc));
                        l.add(o);
                    } else if (f.getType().equals("b")) {//polygon pid
                        Grid g = new Grid(f.getFilePath() + File.separator + "polygons");
                        if (g != null) {
                            float[] vs = g.getValues(new double[][]{{lng, lat}});
                            String pid = f.getLayerPid() + ":" + gc.getId() + ":" + ((int) vs[0]);
                            l.add(getObjectByPid(pid));
                        }
                    }
                }
            }
        }
        if (l.size() > 0) {
            return l.get(0);
        } else {
            return null;
        }
    }

    @Override
    public List<Objects> getNearestObjectByIdAndLocation(String fid, int limit, Double lng, Double lat) {
        logger.info("Getting " + limit + " nearest objects in field fid = " + fid + " to loc: (" + lng + ", " + lat + ") ");

        String sql = "select fid, name, \"desc\", pid, id, ST_AsText(the_geom) as geometry, " +
                "ST_DistanceSphere(ST_SETSRID(ST_Point( ? , ? ),4326), the_geom) as distance, " +
                "degrees(Azimuth( ST_SETSRID(ST_Point( ? , ? ),4326), the_geom)) as degrees, " +
                "area_km " +
                "from objects where fid= ? order by the_geom <#> st_setsrid(st_makepoint( ? , ? ),4326) limit ? ";

        List<Objects> objects = jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Objects.class), lng, lat, lng, lat, fid, lng, lat, limit);
        updateObjectWms(objects);
        return objects;
    }

    @Override
    public List<Objects> getObjectByFidAndName(String fid, String name) {
        logger.info("Getting object info for fid = " + fid + " and name: (" + name + ") ");
        String sql = "select o.pid, o.id, o.name, o.desc as description, o.fid as fid, f.name as fieldname, o.bbox, " +
                "o.area_km, ST_AsText(the_geom) as geometry, GeometryType(the_geom) as featureType from objects o, fields f where o.fid = ? and o.name ilike ? and o.fid = f.id";
        List<Objects> objects = jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Objects.class), fid, name);
        updateObjectWms(objects);
        return objects;
    }

    private String getGridPolygonWms(String layername, int n) {
        return layerIntersectDao.getConfig().getGeoserverUrl() + gridPolygonWmsUrl.replace(SUB_LAYERNAME, layername)
                + formatSld(gridPolygonSld, layername, String.valueOf(n - 1), String.valueOf(n), String.valueOf(n), String.valueOf(n + 1));
    }

    private String getGridClassWms(String layername, GridClass gc) {
        return layerIntersectDao.getConfig().getGeoserverUrl()
                + gridPolygonWmsUrl.replace(SUB_LAYERNAME, layername)
                + formatSld(gridClassSld, layername, String.valueOf(gc.getMinShapeIdx() - 1), String.valueOf(gc.getMinShapeIdx()), String.valueOf(gc.getMaxShapeIdx()),
                String.valueOf(gc.getMaxShapeIdx() + 1));
    }

    private String formatSld(String sld, String layername, String min_minus_one, String min, String max, String max_plus_one) {
        return sld.replace(SUB_LAYERNAME, layername).replace(SUB_MIN_MINUS_ONE, min_minus_one).replace(SUB_MIN, min).replace(SUB_MAX, max).replace(SUB_MAX_PLUS_ONE, max_plus_one);
    }

    private void updateObjectWms(List<Objects> objects) {
        for (Objects o : objects) {
            String wmsurl = objectWmsUrl.replace("<pid>", o.getPid());
            //Points
            if (o.getArea_km() == null) {
                logger.error("area_km cannot be null. wmsurl may be incorect.", new Exception("area_km is null"));
            } else if (o.getArea_km() == 0) {
                wmsurl = wmsurl.replace("ALA:Objects", "ALA:Points");
            }
            o.setWmsurl(layerIntersectDao.getConfig().getGeoserverUrl() + wmsurl);
        }
    }

    private HashMap<String, Object> getGridIndexEntry(String path, String objectId) {
        HashMap<String, Object> map = new HashMap<String, Object>();
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(path + ".wkt.index.dat", "r");

            int s2 = Integer.parseInt(objectId);

            // it is all in order, seek to the record
            int recordSize = 4 * 7; // 2 int + 5 float
            int start = raf.readInt();
            raf.seek(recordSize * (s2 - start));

            map.put("gn", raf.readInt());
            map.put("charoffset", raf.readInt());
            map.put("minx", raf.readFloat());
            map.put("miny", raf.readFloat());
            map.put("maxx", raf.readFloat());
            map.put("maxy", raf.readFloat());
            map.put("area", raf.readFloat());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                if (raf != null) {
                    raf.close();
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        return map;
    }

    @Override
    public List<Objects> getObjectsByIdAndArea(String id, Integer limit, String wkt) {
        String sql = "select fid, name, \"desc\", pid, id, ST_AsText(the_geom) as geometry, GeometryType(the_geom) as featureType, area_km from objects where fid= ? and " +
                "ST_Within(the_geom, ST_GeomFromText( ? , 4326)) limit ? ";

        List<Objects> objects = jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Objects.class), id, wkt, limit);
        updateObjectWms(objects);
        return objects;
    }

    @Override
    public List<Objects> getObjectsByIdAndIntersection(String id, Integer limit, LayerFilter layerFilter) {
        String world = "POLYGON((-180 -90,-180 90,180 90,180 -90,-180 -90))";
        List<Objects> objects = getObjectsByIdAndArea(id, Integer.MAX_VALUE, world);

        double[][] points = new double[objects.size()][2];
        for (int i = 0; i < objects.size(); i++) {
            try {
                String[] s = objects.get(i).getGeometry().substring("POINT(".length(), objects.get(i).getGeometry().length() - 1).split(" ");
                points[i][0] = Double.parseDouble(s[0]);
                points[i][1] = Double.parseDouble(s[1]);
            } catch (Exception e) {
                // don't intersect this one
                points[i][0] = Integer.MIN_VALUE;
                points[i][1] = Integer.MIN_VALUE;
            }
        }

        // sampling
        ArrayList<String> sample = layerIntersectDao.sampling(new String[]{layerFilter.getLayername()}, points);

        // filter
        List<Objects> matched = new ArrayList<Objects>();
        String[] sampling = sample.get(0).split("\n");
        IntersectionFile f = layerIntersectDao.getConfig().getIntersectionFile(layerFilter.getLayername());
        if (f != null && (f.getType().equals("a") || f.getType().equals("b"))) {
            String target = f.getClasses().get((int) layerFilter.getMinimum_value()).getName();
            for (int i = 0; i < sampling.length; i++) {
                if (sampling[i].length() > 0) {
                    if (sampling[i].equals(target)) {
                        matched.add(objects.get(i));
                    }
                }
            }
        } else {
            for (int i = 0; i < sampling.length; i++) {
                if (sampling[i].length() > 0) {
                    double v = Double.parseDouble(sampling[i]);
                    if (v >= layerFilter.getMinimum_value() && v <= layerFilter.getMaximum_value()) {
                        matched.add(objects.get(i));
                    }
                }
            }
        }

        updateObjectWms(matched);
        return matched;
    }

    @Override
    public List<Objects> getObjectsByIdAndIntersection(String id, Integer limit, String intersectingPid) {
        String sql = "select fid, name, \"desc\", pid, id, ST_AsText(the_geom) as geometry, GeometryType(the_geom) as featureType, area_km from objects, " +
                "(select the_geom as g from Objects where pid = ? ) t where fid= ? and ST_Within(the_geom, g) limit ? ";

        List<Objects> objects = jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Objects.class), intersectingPid, id, limit);
        updateObjectWms(objects);

        return objects;
    }

    @Override
    public String createUserUploadedObject(String wkt, String name, String description, String userid) {
        return createUserUploadedObject(wkt, name, description, userid, true);
    }

    @Transactional
    @Override
    public String createUserUploadedObject(String wkt, String name, String description, String userid, boolean namesearch) {

        double area_km = SpatialUtil.calculateArea(wkt) / 1000.0 / 1000.0;

        try {
            int object_id = jdbcTemplate.queryForObject("SELECT nextval('objects_id_seq'::regclass)", Integer.class);
            int metadata_id = jdbcTemplate.queryForObject("SELECT nextval('uploaded_objects_metadata_id_seq'::regclass)", Integer.class);

            // Insert shape into geometry table
            String sql = "INSERT INTO objects (pid, id, name, \"desc\", fid, the_geom, namesearch, bbox, area_km) " +
                    "values (?, ? , ?, ?, ?, ST_GeomFromText(?, 4326), ?, ST_AsText(Box2D(ST_GeomFromText(?, 4326))), ?)";
            jdbcTemplate.update(sql, object_id, metadata_id, name, description, IntersectConfig.getUploadedShapesFieldId(), wkt, namesearch, wkt, area_km);

            // Now write to metadata table
            String sql2 = "INSERT INTO uploaded_objects_metadata (pid, id, user_id, time_last_updated) values (?, ?, ?, now())";
            jdbcTemplate.update(sql2, object_id, metadata_id, userid);

            return Integer.toString(object_id);
        } catch (DataAccessException ex) {
            throw new IllegalArgumentException("Error writing to database. Check validity of wkt.", ex);
        }
    }

    @Override
    @Transactional
    public boolean updateUserUploadedObject(int pid, String wkt, String name, String description, String userid) {

        if (!shapePidIsForUploadedShape(pid)) {
            throw new IllegalArgumentException("Supplied pid does not match an uploaded shape.");
        }

        try {
            double area_km = SpatialUtil.calculateArea(wkt) / 1000.0 / 1000.0;

            // First update metadata table
            String sql = "UPDATE uploaded_objects_metadata SET user_id = ?, time_last_updated = now() WHERE pid = ?";
            jdbcTemplate.update(sql, userid, Integer.toString(pid));

            // Then update objects table
            String sql2 = "UPDATE objects SET the_geom = ST_GeomFromText(?, 4326), " +
                    "bbox = ST_AsText(Box2D(ST_GeomFromText(?, 4326))), name = ?, \"desc\" = ?, area_km = ? where pid = ?";
            int rowsUpdated = jdbcTemplate.update(sql2, wkt, wkt, name, description, area_km, Integer.toString(pid));
            return (rowsUpdated > 0);
        } catch (DataAccessException ex) {
            throw new IllegalArgumentException("Error writing to database. Check validity of wkt.", ex);
        }
    }

    @Override
    @Transactional
    public boolean deleteUserUploadedObject(int pid) {
        if (!shapePidIsForUploadedShape(pid)) {
            throw new IllegalArgumentException("Supplied pid does not match an uploaded shape.");
        }

        String sql = "DELETE FROM uploaded_objects_metadata WHERE pid = ?; DELETE FROM objects where pid = ?";
        int rowsAffected = jdbcTemplate.update(sql, Integer.toString(pid), Integer.toString(pid));
        return (rowsAffected > 0);
    }

    @Async
    public void updateObjectNames() {
        String sql = "INSERT INTO obj_names (name) SELECT lower(objects.name) FROM fields, objects " +
                "LEFT OUTER JOIN obj_names ON lower(objects.name)=obj_names.name WHERE obj_names.name IS NULL" +
                " AND fields.namesearch = true AND fields.id = objects.fid GROUP BY lower(objects.name);" +
                " UPDATE objects SET name_id=obj_names.id FROM obj_names WHERE name_id IS NULL AND lower(objects.name)=obj_names.name;";
        jdbcTemplate.update(sql);
    }

    private boolean shapePidIsForUploadedShape(int pid) {
        String sql = "SELECT * from uploaded_objects_metadata WHERE pid = ?";
        List<Map<String, Object>> queryResult = jdbcTemplate.queryForList(sql, Integer.toString(pid));
        if (queryResult == null || queryResult.isEmpty()) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public int createPointOfInterest(String objectId, String name, String type, Double latitude, Double longitude, Double bearing, String userId, String description, Double focalLength) {
        String sql = "INSERT INTO points_of_interest (id, object_id, name, type, latitude, longitude, bearing, " +
                "user_id, description, focal_length_millimetres, the_geom) " +
                "VALUES (DEFAULT, ?, ?, ?, ?, ?, ?, ?, ?, ?, ST_SetSRID(ST_MakePoint(?, ?),4326))";
        jdbcTemplate.update(sql, objectId, name, type, latitude, longitude, bearing, userId, description, focalLength, longitude, latitude);

        // get pid and id of new object
        String sql2 = "SELECT MAX(id) from points_of_interest";
        int id = jdbcTemplate.queryForObject(sql2, Integer.class);
        return id;
    }

    @Override
    public boolean updatePointOfInterest(int id, String objectId, String name, String type, Double latitude, Double longitude, Double bearing, String userId, String description, Double focalLength) {
        String sql = "UPDATE points_of_interest SET object_id = ?, name = ?, type = ?, latitude = ?, longitude = ?, " +
                "bearing = ?, user_id = ?, description = ?, focal_length_millimetres = ? WHERE id = ?; "
                + "UPDATE points_of_interest SET the_geom = ST_SetSRID(ST_MakePoint(longitude, latitude),4326) WHERE id = ?";
        int rowsUpdated = jdbcTemplate.update(sql, objectId, name, type, latitude, longitude, bearing, userId, description, focalLength, id, id);
        return (rowsUpdated > 0);
    }

    @Override
    public Map<String, Object> getPointOfInterestDetails(int id) {
        String sql = "SELECT id, object_id, name, type, latitude, longitude, bearing, user_id, description, " +
                "focal_length_millimetres from points_of_interest WHERE id = ?";
        Map<String, Object> poiDetails = jdbcTemplate.queryForMap(sql, id);
        if (poiDetails.isEmpty()) {
            throw new IllegalArgumentException("Invalid point of interest id");
        }

        return poiDetails;
    }

    @Override
    public boolean deletePointOfInterest(int id) {
        String sql = "DELETE FROM points_of_interest WHERE id = ?;";
        int rowsAffected = jdbcTemplate.update(sql, id);
        return (rowsAffected > 0);
    }

    @Override
    public List<Objects> getObjectsWithinRadius(String fid, double latitude, double longitude, double radiusKm) {
        String sql = "SELECT o.pid, o.id, o.name, o.desc AS description, o.fid AS fid, f.name AS fieldname, o.bbox, " +
                "o.area_km, GeometryType(o.the_geom) as featureType FROM objects o, fields f WHERE o.fid = ? AND o.fid = f.id AND " +
                "ST_DWithin(ST_GeographyFromText('POINT(" + longitude + " " + latitude + ")'), geography(the_geom), ?, true)";
        List<Objects> l = jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Objects.class), fid, radiusKm * 1000);
        updateObjectWms(l);
        return l;
    }

    @Override
    public List<Objects> getObjectsIntersectingWithGeometry(String fid, String wkt) {
        String sql = "SELECT o.pid, o.id, o.name, o.desc AS description, o.fid AS fid, f.name AS fieldname, " +
                "o.bbox, o.area_km from search_objects_by_geometry_intersect(?, ST_GeomFromText(?, 4326)) o, fields f WHERE o.fid = f.id";
        List<Objects> l = jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Objects.class), fid, wkt);
        updateObjectWms(l);
        return l;
    }

    @Override
    public List<Objects> getObjectsIntersectingWithObject(String fid, String objectPid) {
        String sql = "SELECT o.pid, o.id, o.name, o.desc AS description, o.fid AS fid, f.name AS fieldname, " +
                "o.bbox, o.area_km FROM search_objects_by_geometry_intersect(?, (SELECT the_geom FROM objects WHERE pid = ?)) o, fields f WHERE o.fid = f.id";
        List<Objects> l = jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Objects.class), fid, objectPid);
        updateObjectWms(l);
        return l;
    }

    @Override
    public List<Map<String, Object>> getPointsOfInterestWithinRadius(double latitude, double longitude, double radiusKm) {
        String sql = "SELECT id, object_id, name, type, latitude, longitude, bearing, user_id, description, " +
                "focal_length_millimetres from points_of_interest " +
                "WHERE ST_DWithin(ST_GeographyFromText('POINT(" + longitude + " " + latitude + ")'), geography(the_geom), ?,true)";
        List<Map<String, Object>> l = jdbcTemplate.queryForList(sql, radiusKm * 1000);
        return l;
    }

    @Override
    public List<Map<String, Object>> pointsOfInterestGeometryIntersect(String wkt) {
        String sql = "SELECT id, object_id, name, type, latitude, longitude, bearing, user_id, description, " +
                "focal_length_millimetres from points_of_interest WHERE ST_Intersects(ST_GeomFromText(?, 4326), the_geom)";
        List<Map<String, Object>> l = jdbcTemplate.queryForList(sql, wkt);
        return l;
    }

    @Override
    public List<Map<String, Object>> pointsOfInterestObjectIntersect(String objectPid) {
        String sql = "SELECT id, object_id, name, type, latitude, longitude, bearing, user_id, description, " +
                "focal_length_millimetres from points_of_interest WHERE ST_Intersects((SELECT the_geom FROM objects where pid = ?), the_geom)";
        List<Map<String, Object>> l = jdbcTemplate.queryForList(sql, objectPid);
        return l;
    }

    @Override
    public int getPointsOfInterestWithinRadiusCount(double latitude, double longitude, double radiusKm) {
        String sql = "SELECT count(*) from points_of_interest " +
                "WHERE ST_DWithin(ST_GeographyFromText('POINT(" + longitude + " " + latitude + ")'), geography(the_geom), ?, true)";
        return jdbcTemplate.queryForObject(sql, Integer.class,radiusKm * 1000);
    }

    @Override
    public int pointsOfInterestGeometryIntersectCount(String wkt) {
        String sql = "SELECT count(*) from points_of_interest WHERE ST_Intersects(ST_GeomFromText(?, 4326), the_geom)";
        return jdbcTemplate.queryForObject(sql, Integer.class, wkt);
    }

    @Override
    public int pointsOfInterestObjectIntersectCount(String objectPid) {
        String sql = "SELECT count(*) from points_of_interest WHERE ST_Intersects((SELECT the_geom FROM objects where pid = ?), the_geom)";
        return jdbcTemplate.queryForObject(sql, Integer.class, objectPid);
    }

    @Override
    public Objects intersectObject(String pid, double latitude, double longitude) {
        String sql = "SELECT o.pid, o.id, o.name, o.desc AS description, o.fid AS fid, f.name AS fieldname, o.bbox, " +
                "o.area_km, GeometryType(o.the_geom) as featureType FROM objects o, fields f WHERE o.pid = ? AND o.fid = f.id AND " +
                "ST_Intersects(the_geom, ST_GeomFromText('POINT(" + longitude + " " + latitude + ")', 4326))";
        List<Objects> l = jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Objects.class), pid);
        updateObjectWms(l);
        if (l.size() > 0) {
            return l.get(0);
        } else {
            return null;
        }
    }

    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
