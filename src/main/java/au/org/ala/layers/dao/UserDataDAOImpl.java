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

import au.org.ala.layers.dto.IntersectionFile;
import au.org.ala.layers.dto.Ud_header;
import au.org.ala.layers.intersect.SimpleRegion;
import au.org.ala.layers.intersect.SimpleShapeFile;
import au.org.ala.layers.legend.Facet;
import au.org.ala.layers.legend.QueryField;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.io.*;
import java.util.*;


@Service("userDataDao")
public class UserDataDAOImpl implements UserDataDAO {

    /**
     * log4j logger
     */
    private static final Logger logger = Logger.getLogger(UserDataDAOImpl.class);
    private JdbcTemplate jdbcTemplate;
    private NamedParameterJdbcTemplate jdbcParameterTemplate;

    @Resource(name = "layerIntersectDao")
    private LayerIntersectDAO layerIntersectDao;

    @Resource(name = "dataSource")
    public void setDataSource(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.jdbcParameterTemplate = new NamedParameterJdbcTemplate(dataSource);
    }

    @Override
    public Ud_header put(String user_id, String record_type, String description, String metadata, String data_path, String analysis_id) {
        String sql_insert = "INSERT INTO ud_header (user_id,record_type,description,metadata,data_path,analysis_id,upload_dt) "
                + " VALUES (?,?,?,?,?,?,?);";

        Date upload_dt = new Date(System.currentTimeMillis());
        int rows = jdbcTemplate.update(
                sql_insert,
                user_id, record_type, description, metadata, data_path, analysis_id, upload_dt);


        if (rows > 0) {
            String sql_select = "SELECT * FROM ud_header WHERE user_id = ? AND upload_dt = ?";

            Ud_header ud_header = (Ud_header) jdbcTemplate.queryForObject(
                    sql_select,
                    new BeanPropertyRowMapper(Ud_header.class),
                    user_id, upload_dt);

            return ud_header;
        }

        return null;
    }

    @Override
    public Ud_header update(Long ud_header_id, String user_id, String record_type, String description, String metadata, String data_path, String analysis_id) {
        String sql_update = "UPDATE ud_header SET user_id=?, record_type=?, description=?, metadata=?, data_path=?, " +
                "analysis_id=?, upload_dt=? WHERE ud_header_id=?";

        Date upload_dt = new Date(System.currentTimeMillis());
        int rows = jdbcTemplate.update(
                sql_update,
                user_id, record_type, description, metadata, data_path, analysis_id, upload_dt, ud_header_id);


        if (rows > 0) {
            String sql_select = "SELECT * FROM ud_header WHERE user_id = ? AND ud_header_id = ?";

            Ud_header ud_header = (Ud_header) jdbcTemplate.queryForObject(
                    sql_select,
                    new BeanPropertyRowMapper(Ud_header.class),
                    user_id, ud_header_id);

            return ud_header;
        }

        return null;
    }

    @Override
    public Ud_header get(Long ud_header_id) {
        String sql_select = "SELECT * FROM ud_header WHERE ud_header_id = ? ;";

        Ud_header ud_header = (Ud_header) jdbcTemplate.queryForObject(
                sql_select,
                new BeanPropertyRowMapper(Ud_header.class),
                ud_header_id);
        return ud_header;
    }

    @Override
    public boolean delete(Long ud_header_id) {
        String sqlDeleteData = "DELETE FROM ud_data_x WHERE ud_header_id = :ud_header_id ;";
        String sqlDeleteHeader = "DELETE FROM ud_header WHERE ud_header_id = :ud_header_id ;";

        try {
            Map params = new HashMap();
            params.put("ud_header_id", ud_header_id);
            jdbcParameterTemplate.update(sqlDeleteData, params);
            jdbcParameterTemplate.update(sqlDeleteHeader, params);
            return true;
        } catch (Exception e) {
            logger.error("failed to delete user data for ud_header_id=" + ud_header_id);
        }

        return false;
    }

    @Override
    public String[] getStringArray(String header_id, String ref) {
        try {
            return (String[]) get(header_id, ref, "StringArray");
        } catch (Exception e) {
            logger.error("failed to get StringArray", e);
        }
        return null;
    }

    @Override
    public boolean[] getBooleanArray(String header_id, String ref) {
        try {
            return (boolean[]) get(header_id, ref, "BooleanArray");
        } catch (Exception e) {
            logger.error("failed to get BooleanArray", e);
        }
        return null;
    }

    @Override
    public double[][] getDoublesArray(String header_id, String ref) {
        try {
            return (double[][]) get(header_id, ref, "DoublesArray");
        } catch (Exception e) {
            logger.error("failed to get DoublesArray", e);
        }
        return null;
    }

    Object get(String header_id, String ref, String data_type) {

        Long id = Long.parseLong(header_id.split(":")[0]);
        String facet_id = (header_id.contains(":")) ? " " + header_id.split(":")[1] : "";

        String sql = "SELECT * FROM ud_data_x WHERE ud_header_id = ? AND ref = ? AND data_type = ?;";

        try {
            Map<String, Object> o = jdbcTemplate.queryForMap(sql, id, ref + facet_id, data_type);

            if (o != null) {
                try {
                    ByteArrayInputStream bytes = new ByteArrayInputStream((byte[]) o.get("data"));
                    ObjectInputStream obj = new ObjectInputStream(bytes);
                    return obj.readObject();
                } catch (Exception e) {
                    logger.error("failed to get " + data_type + " for " + header_id + ", " + ref, e);
                }
            }
        } catch (EmptyResultDataAccessException e) {
            //don't care
        }
        return null;
    }

    @Override
    public boolean setStringArray(String header_id, String ref, String[] data) {
        return set(header_id, ref, "StringArray", data);
    }

    @Override
    public boolean setBooleanArray(String header_id, String ref, boolean[] data) {
        return set(header_id, ref, "BooleanArray", data);
    }

    @Override
    public boolean setDoublesArray(String header_id, String ref, double[][] data) {
        return set(header_id, ref, "DoublesArray", data);
    }

    private boolean set(String header_id, String ref, String data_type, Object o) {
        Long id = Long.parseLong(header_id.split(":")[0]);
        String facet_id = (header_id.contains(":")) ? " " + header_id.split(":")[1] : "";

        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            ObjectOutputStream obj = new ObjectOutputStream(bytes);
            obj.writeObject(o);
            obj.flush();

            try {
                String sql_delete = "DELETE FROM ud_data_x WHERE ud_header_id = ? AND ref = ? AND data_type = ?;";

                int deleted = jdbcTemplate.update(sql_delete, id, ref + facet_id, data_type);

                String sql_insert = "INSERT INTO ud_data_x (ud_header_id,ref,data_type, data) "
                        + " VALUES ( ?, ?, ?, ?);";

                int inserted = jdbcTemplate.update(sql_insert,
                        id, ref + facet_id, data_type, bytes.toByteArray());

                return inserted > 0;
            } catch (Exception e) {
                logger.error("failed to set ud_data_x for " + header_id + ", " + ref, e);
            }
        } catch (Exception e) {
            logger.error("failed to write bytes for: " + header_id + ", " + ref, e);
        }

        return false;
    }

    @Override
    public List<Ud_header> list(String user_id) {

        String sql = "SELECT * FROM Ud_header WHERE user_id = ?";

        List<Ud_header> ud_headers = (List<Ud_header>) jdbcTemplate.queryForObject(
                sql,
                new BeanPropertyRowMapper(Ud_header.class),
                user_id);

        return ud_headers;
    }

    @Override
    public boolean setDoubleArray(String header_id, String ref, double[] data) {
        return set(header_id, ref, "DoubleArray", data);
    }

    @Override
    public boolean setQueryField(String ud_header_id, String ref, QueryField qf) {
        return set(ud_header_id, ref, "QueryField", qf);
    }

    @Override
    public double[] getDoubleArray(String header_id, String ref) {
        try {
            if (header_id.contains(":")) {
                //facet
                boolean[] valid = getBooleanArray(header_id.split(":")[0], header_id.split(":")[1]);
                int count_valid = 0;
                for (boolean b : valid) {
                    if (b) {
                        count_valid++;
                    }
                }
                double[] allpoints = (double[]) get(header_id.split(":")[0], ref, "DoubleArray");
                double[] points = new double[count_valid * 2];
                for (int i = 0, pos = 0; i < allpoints.length; i += 2) {
                    if (valid[i / 2]) {
                        points[pos] = allpoints[i];
                        points[pos + 1] = allpoints[i + 1];
                        pos += 2;
                    }
                }
                return points;
            } else {
                return (double[]) get(header_id, ref, "DoubleArray");
            }
        } catch (Exception e) {
            logger.error("failed to get DoubleArray", e);
        }
        return null;
    }

    @Override
    public QueryField getQueryField(String header_id, String ref) {
        String id = header_id.split(":")[0];
        String facet = (header_id.contains(":")) ? header_id.split(":")[1] : "";

        QueryField qf = null;
        try {
            qf = (QueryField) get(header_id, ref, "QueryField");
        } catch (Exception e) {
            logger.error("failed to get QueryField", e);
        }

        if (qf == null) {
            //if this is a facet, get the data from the parent
            if (facet.length() > 0) {

                //facet
                boolean[] valid = getBooleanArray(header_id.split(":")[0], header_id.split(":")[1]);

                QueryField qfSource = getQueryField(id, ref);
                QueryField qfFacet = new QueryField();
                qfFacet.setDisplayName(qfSource.getDisplayName());
                qfFacet.setName(qfSource.getName());

                try {
                    for (int i = 0; i < valid.length; i++) {
                        if (valid[i]) {
                            qfFacet.add(qfSource.getAsString(i));
                        }
                    }
                } catch (Exception e) {
                    //likely that ref does not actually exist for intersection
                    logger.error("invalid QueryField for id: " + header_id + " ref: " + ref + " does the ref exist for intersection?", e);
                }
                qfFacet.store();

                setQueryField(header_id, ref, qf);

                return qfFacet;
            } else {

                //build qf
                double[] flatPoints = getDoubleArray(header_id, "points");

                double[][] points = new double[flatPoints.length / 2][2];
                int pos = 0;
                for (int i = 0; i < points.length; i++) {
                    points[pos][0] = flatPoints[i * 2];
                    points[pos][1] = flatPoints[i * 2 + 1];
                    pos++;
                }
                List<String> s = layerIntersectDao.sampling(new String[]{ref}, points);
                String[] a = s.get(0).split("\n");
                qf = new QueryField(ref);
                for (int i = 0; i < a.length; i++) {
                    qf.add(a[i]);
                }
                qf.store();

                //can we get a better display name?
                IntersectionFile f = layerIntersectDao.getConfig().getIntersectionFile(ref);
                if (f != null) {
                    qf.setDisplayName(f.getFieldName());
                }
                setQueryField(header_id, ref, qf);
            }
        }

        return qf;
    }

    @Override
    public boolean setMetadata(long header_id, Map data) {
        String sql_insert = "UPDATE ud_header SET metadata = ? WHERE ud_header_id = ? ;";

        Date upload_dt = new Date(System.currentTimeMillis());
        int rows = jdbcTemplate.update(
                sql_insert,
                JSONObject.fromObject(data).toString(), header_id);
        return rows == 1;
    }

    @Override
    public Map getMetadata(long header_id) {
        ObjectMapper om = new ObjectMapper();
        try {
            return om.readValue(get(header_id).getMetadata(), Map.class);
        } catch (IOException e) {
            logger.error("error getting metadata for header_id: " + header_id, e);
        }
        return null;
    }

    @Override
    public List<String> listData(String ud_header_id, String data_type) {
        Long id = Long.parseLong(ud_header_id.split(":")[0]);
        String facet_id = (ud_header_id.contains(":")) ? ud_header_id.split(":")[1] : "";

        String sql;
        List<Map<String, Object>> l;

        if (facet_id.length() > 0) {
            sql = "SELECT ref FROM ud_data_x WHERE ud_header_id = ? AND data_type = ? AND ref like ? ;";
            l = jdbcTemplate.queryForList(sql, id, data_type, "% " + facet_id);
        } else {
            sql = "SELECT ref FROM ud_data_x WHERE ud_header_id = ? AND data_type = ? AND ref not like '% %' ; ";
            l = jdbcTemplate.queryForList(sql, id, data_type);
        }

        ArrayList<String> refs = new ArrayList<String>();
        for (Map<String, Object> m : l) {
            //remove facet_id from ref values
            refs.add(((String) m.get("ref")).replace(" " + facet_id, ""));
        }
        return refs;
    }

    @Override
    public Ud_header facet(String id, List<String> new_facets, String new_wkt) {

        SimpleRegion sr = null;
        double[] points = null;

        String ud_header_id = id.split(":")[0];
        String existing_facet = id.contains(":") ? id.split(":")[1] : null;

        //setup
        ArrayList<Facet> facets = new ArrayList<Facet>();
        for (int i = 0; i < new_facets.size(); i++) {
            facets.add(Facet.parseFacet(new_facets.get(i)));
        }
        ArrayList<List<QueryField>> facetFields = new ArrayList<List<QueryField>>();
        for (int k = 0; k < facets.size(); k++) {
            Facet f = facets.get(k);
            String[] fields = f.getFields();
            List<QueryField> qf = new ArrayList<QueryField>();
            for (int j = 0; j < fields.length; j++) {
                qf.add(getQueryField(ud_header_id, fields[j]));
            }
            facetFields.add(qf);
        }
        if (new_wkt != null) {
            sr = SimpleShapeFile.parseWKT(new_wkt);
        }
        points = getDoubleArray(ud_header_id, "points");
        boolean[] existing_valid = null;
        if (existing_facet != null) {
            existing_valid = getBooleanArray(ud_header_id, existing_facet);
        }

        //per record test
        boolean[] valid = new boolean[points.length / 2];
        int count = 0;
        boolean valid_sr;
        boolean valid_existing;
        for (int i = 0; i < valid.length; i++) {
            int sum = 0;
            for (int j = 0; j < facets.size(); j++) {
                if (facets.get(j).isValid(facetFields.get(j), i)) {
                    sum++;
                }

            }

            valid_sr = (sr == null || sr.isWithin(points[i * 2], points[i * 2 + 1]));
            valid_existing = (existing_valid == null || existing_valid[i]);

            valid[i] = (sum == facets.size()) && valid_sr && valid_existing;
            if (valid[i]) {
                count++;
            }
        }

        //put into database
        String next_facet_id = String.valueOf(System.currentTimeMillis());
        setBooleanArray(ud_header_id, next_facet_id, valid);

        //store derived facets
        List<String> refs = listData(ud_header_id, "QueryField");
        for (int i = 0; i < refs.size(); i++) {
            getQueryField(ud_header_id + ":" + next_facet_id, refs.get(i));
        }

        //return it
        Ud_header ret = get(Long.valueOf(ud_header_id));
        ret.setFacet_id(next_facet_id);

        //add facet count to metadata so it is returned
        String metadata = ret.getMetadata();
        JSONObject jo = JSONObject.fromObject(metadata);
        jo.put("number_of_records", count);
        ret.setMetadata(jo.toString());

        return ret;
    }

    @Override
    public String getSampleZip(String id, String fields) {
        //get everything
        ArrayList<QueryField> qfs = new ArrayList<QueryField>();

        //add everything + fields
        List<String> in = listData(id, "QueryField");

        if (in != null) {
            //put original header records in order
            Collections.sort(in, new Comparator<String>() {

                @Override
                public int compare(String o1, String o2) {

                    if (o1.startsWith("__f") && o2.startsWith("__f")) {
                        int i1 = Integer.MAX_VALUE;
                        int i2 = Integer.MAX_VALUE;
                        try {
                            i1 = Integer.parseInt(o1.substring(3));
                        } catch (Exception e) {
                        }
                        try {
                            i2 = Integer.parseInt(o2.substring(3));
                        } catch (Exception e) {
                        }
                        return i1 - i2;
                    } else if (o1.startsWith("__f")) {
                        return -1;
                    } else if (o2.startsWith("__f")) {
                        return 1;
                    } else {
                        return o1.compareTo(o2);
                    }
                }
            });
            for (int i = 0; i < in.size(); i++) {
                //add only original header fields
                if (in.get(i).startsWith("__f")) {
                    qfs.add(getQueryField(id, in.get(i)));
                }
            }
        }
        if (fields != null) {
            String[] fs = fields.split(",");
            for (int i = 0; i < fs.length; i++) {
                //if (!in.contains(fs[i])) {
                qfs.add(getQueryField(id, fs[i]));
                //}
            }
        }

        //make csv
        StringBuilder sb = new StringBuilder();
        //header
        for (int i = 0; i < qfs.size(); i++) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(qfs.get(i).getDisplayName());
        }
        //rows
        double[] points = getDoubleArray(id, "points");
        int size = points.length / 2;
        for (int i = 0; i < size; i++) {
            sb.append("\r\n");
            for (int j = 0; j < qfs.size(); j++) {
                QueryField q = qfs.get(j);

                //if an intersection fails or a supplied field is invalid, return empty values
                String s = "";
                try {
                    s = q.getAsString(i);
                } catch (Exception e) {
                }

                if (j > 0) {
                    sb.append(",");
                }

                if (s != null) {
                    sb.append("\"").append(s.replace("\"", "\"\"")).append("\"");
                }
            }
        }

        return sb.toString();
    }

    @Override
    public List<Ud_header> searchDescAndTypeOr(String desc, String record_type, String user_id, String data_path,
                                               String analysis_id, int start, int limit) {
        Map params = new HashMap();

        String and = "";

        if (desc != null) {
            and += " description ilike :desc ";
            params.put("desc", desc);
        }

        if (record_type != null) {
            if (!and.isEmpty()) and += " AND ";
            and += " record_type = :record_type";
            params.put("record_type", record_type);
        }

        String or = "";

        if (user_id != null) {
            or = " user_id = :user_id ";
            params.put("user_id", user_id);
        }

        if (data_path != null) {
            if (!or.isEmpty()) or += " OR ";
            or += " data_path = :data_path ";
            params.put("data_path", data_path);
        }

        if (analysis_id != null) {
            if (!or.isEmpty()) or += " OR ";
            or += " analysis_id = :analysis_id ";
            params.put("analysis_id", analysis_id);
        }

        String where = "";

        if (!and.isEmpty()) {
            where += " " + and;
        }

        if (!or.isEmpty()) {
            if (!where.isEmpty()) where += " AND ";
            where += " (" + or + ") ";
        }

        String sql = "SELECT * FROM ud_header";

        if (!where.isEmpty()) {
            sql += " WHERE " + where;
        }

        sql += " offset :offset limit :limit";
        params.put("offset", start);
        params.put("limit", limit);

        return jdbcParameterTemplate.query(sql, params, BeanPropertyRowMapper.newInstance(Ud_header.class));
    }


}
