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

import au.org.ala.layers.dto.Field;
import au.org.ala.layers.dto.Layer;
import au.org.ala.layers.util.Util;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author ajay
 */
@Service("fieldDao")
public class FieldDAOImpl implements FieldDAO {

    /**
     * log4j logger
     */
    private static final Logger logger = Logger.getLogger(FieldDAOImpl.class);

    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private JdbcTemplate jdbcTemplate;
    private SimpleJdbcInsert insertField;
    private String selectLayerSql;

    @Resource(name = "layerIntersectDao")
    private LayerIntersectDAO layerIntersectDao;
    @Resource(name = "layerDao")
    private LayerDAO layerDao;

    @PostConstruct
    private void init() {
        StringBuilder sb = new StringBuilder();
        for (String key : new Layer().toMap().keySet()) {
            sb.append(",l.").append(key).append(" as layer_").append(key);
        }
        selectLayerSql = sb.toString();
    }

    @Resource(name = "dataSource")
    public void setDataSource(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        this.insertField = new SimpleJdbcInsert(dataSource).withTableName("fields")
                .usingColumns("id", "name", "\"desc\"", "sname", "sdesc", "sid", "addtomap", "\"intersect\"",
                        "defaultlayer", "enabled", "layerbranch", "analysis", "indb", "spid", "namesearch", "type", "last_update");
    }

    @Override
    public List<Field> getFields() {
        return getFields(false);
    }

    @Override
    public List<Field> getFields(boolean enabledFieldsOnly) {
        logger.info("Getting a list of all fields");
        String sql = "select * from fields";
        if (enabledFieldsOnly) {
            sql += " where enabled=true";
        }
        return jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Field.class));
    }

    @Override
    public Field getFieldById(String id) {
        return getFieldById(id, true);
    }

    @Override
    public Field getFieldById(String id, boolean enabledFieldsOnly) {
        logger.info("Getting enabled field info for id = " + id);
        String sql = "select *, number_of_objects from fields, (select count(*) as number_of_objects from objects where fid = ? ) o where id = ? ";
        if (enabledFieldsOnly) {
            sql += " and enabled=true";
        }
        List<Field> l = jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Field.class), id, id);
        if (l.size() > 0) {
            return l.get(0);
        } else {
            return null;
        }
    }

    @Override
    public List<Field> getFieldsByDB() {
        if (layerIntersectDao.getConfig().getLayerIndexUrl() != null) {
            return layerIntersectDao.getConfig().getFieldsByDB();
        } else {
            logger.info("Getting a list of all enabled fields with indb");
            String sql = "select * from fields where enabled=TRUE and indb=TRUE";
            return jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Field.class));
        }

    }

    @Override
    public synchronized void addField(Field field) {
        logger.info("Add new field for " + field.getName());

        Map<String, Object> parameters = field.toMap();
        parameters.remove("id");
        parameters.remove("layer");

        //calc new fieldId
        String idPrefix = "Contextual".equalsIgnoreCase(layerDao.getLayerById(Integer.parseInt(field.getSpid()), false).getType())
                ? "cl" : "el";

        //test for requested id
        String newId = field.getId();

        if (newId == null || getFieldById(newId) != null) {
            newId = getFieldById(idPrefix + field.getSpid()) == null ? idPrefix + field.getSpid() : null;
            if (newId == null) {
                //calculate next field Id using general form: prefix (n x 1000 + layerId)
                String idEnd = field.getSpid();
                while (idEnd.length() < 3) {
                    idEnd = "0" + idEnd;
                }
                int maxNFound = 0;
                for (Field f : getFields(false)) {
                    if (f.getId().startsWith(idPrefix) && f.getId().endsWith(idEnd)) {
                        if (f.getId().length() - idEnd.length() > 2) {
                            int n = Integer.parseInt(f.getId().substring(2, f.getId().length() - idEnd.length()));
                            if (n > maxNFound) {
                                maxNFound = n;
                            }
                        }
                    }
                }

                newId = idPrefix + (maxNFound + 1) + idEnd;
            }
        }

        parameters.put("id", newId);
        //fix for field 'desc' and 'intersect'
        if (parameters.containsKey("desc")) {
            parameters.put("\"desc\"", parameters.get("desc"));
            parameters.remove("desc");
        }
        if (parameters.containsKey("intersect")) {
            parameters.put("\"intersect\"", parameters.get("intersect"));
            parameters.remove("intersect");
        }

        insertField.execute(parameters);

        field.setId(newId);
    }

    @Override
    public void updateField(Field field) {
        logger.info("Updating field metadata for " + field.getName());

        String sql = "update fields set name=:name, " +
                "\"desc\"=:desc, type=:type, " +
                "spid=:spid, sid=:sid, sname=:sname, " +
                "sdesc=:sdesc, indb=:indb, enabled=:enabled, " +
                "namesearch=:namesearch, defaultlayer=:defaultlayer, " +
                "\"intersect\"=:intersect, layerbranch=:layerbranch, analysis=:analysis," +
                " addtomap=:addtomap where id=:id";

        Map map = field.toMap();
        map.remove("layer");
        namedParameterJdbcTemplate.update(sql, map);
    }

    @Override
    public void delete(String fieldId) {
        Field f = getFieldById(fieldId);

        if (f != null) {
            jdbcTemplate.update("delete from objects where fid=?", f.getId());
            jdbcTemplate.update("delete from fields where id=?", f.getId());
        }
    }

    @Override
    public List<Layer> getLayersByCriteria(String keywords) {
        logger.info("Getting a list of all enabled fields by criteria: " + keywords);
        String sql = "";
        sql += "select f.* " + selectLayerSql + " from fields f inner join layers l on f.spid = l.id || '' where ";
        sql += " l.enabled=true AND f.enabled=true AND ( ";
        sql += "lower(l.keywords) like ? ";
        sql += " or lower(l.displayname) like ? ";

        sql += " or lower(l.name) like ? ";
        sql += " or lower(l.domain) like ? ";
        sql += " or lower(f.name) like ? ";
        sql += ") order by f.name ";

        keywords = "%" + keywords.toLowerCase() + "%";

        List<Map<String, Object>> maps = jdbcTemplate.queryForList(sql, keywords, keywords, keywords, keywords, keywords);

        List<Layer> list = mapsToLayers(maps);

        return list;
    }

    @Override
    public List<Field> getFieldsByCriteria(String keywords) {
        logger.info("Getting a list of all enabled fields by criteria: " + keywords);
        String sql = "";
        sql += "select f.* " + selectLayerSql + " from fields f inner join layers l on f.spid = l.id || '' where ";
        sql += " l.enabled=true AND f.enabled=true AND ( ";
        sql += "lower(l.keywords) like ? ";
        sql += " or lower(l.displayname) like ? ";

        sql += " or lower(l.name) like ? ";
        sql += " or lower(l.domain) like ? ";
        sql += " or lower(f.name) like ? ";
        sql += ") order by f.name ";

        keywords = "%" + keywords.toLowerCase() + "%";

        List<Map<String, Object>> maps = jdbcTemplate.queryForList(sql, keywords, keywords, keywords, keywords, keywords);

        List<Field> list = mapsToFields(maps);

        return list;
    }

    private List<Layer> mapsToLayers(List<Map<String, Object>> maps) {
        List<Layer> list = new ArrayList<Layer>();

        ObjectMapper om = new ObjectMapper();
        om.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        for (Map<String, Object> map : maps) {
            try {
                Map field = new HashMap();
                Map layer = new HashMap();
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    if (entry.getKey().startsWith("layer_"))
                        layer.put(entry.getKey().substring("layer_".length()), entry.getValue());
                    else field.put(entry.getKey(), entry.getValue());
                }
                Field f = om.readValue(om.writeValueAsString(field), Field.class);
                Layer l = om.readValue(om.writeValueAsString(layer), Layer.class);
                Util.updateDisplayPath(l);
                Util.updateMetadataPath(l);
                f.setLayer(l);
                if (layerIntersectDao.getConfig().hasFieldStyles()) {
                    //conditional so as not to break older ingested layers
                    l.setDisplaypath(l.getDisplaypath().replace("&styles=", "") + "&style=" + f.getId() + "_style");
                }
                l.setDisplayname(f.getName());
                l.setPid(f.getId());
                list.add(l);
            } catch (Exception e) {
                logger.error("failed to read field/layer " + map.get("id"), e);
            }
        }

        return list;
    }

    private List<Field> mapsToFields(List<Map<String, Object>> maps) {
        List<Field> list = new ArrayList<Field>();

        ObjectMapper om = new ObjectMapper();
        om.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        for (Map<String, Object> map : maps) {
            try {
                Map field = new HashMap();
                Map layer = new HashMap();
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    if (entry.getKey().startsWith("layer_"))
                        layer.put(entry.getKey().substring("layer_".length()), entry.getValue());
                    else field.put(entry.getKey(), entry.getValue());
                }
                Field f = om.readValue(om.writeValueAsString(field), Field.class);
                Layer l = om.readValue(om.writeValueAsString(layer), Layer.class);
                Util.updateDisplayPath(l);
                Util.updateMetadataPath(l);
                f.setLayer(l);

                l.setDisplaypath(l.getDisplaypath().replace("&styles=", "") + "&style=" + f.getId() + "_style");

                list.add(f);
            } catch (Exception e) {
                logger.error("failed to read field/layer " + map.get("id"), e);
            }
        }

        return list;
    }
}
