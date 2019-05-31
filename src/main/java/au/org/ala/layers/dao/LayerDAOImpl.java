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

import au.org.ala.layers.dto.Layer;
import au.org.ala.layers.util.Util;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.sql.Connection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author ajay
 */
@Service("layerDao")
public class LayerDAOImpl implements LayerDAO {

    /**
     * log4j logger
     */
    private static final Logger logger = Logger.getLogger(LayerDAOImpl.class);
    private JdbcTemplate jdbcTemplate;
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private SimpleJdbcInsert insertLayer;
    private Connection connection;
    private DataSource dataSource;

    @Resource(name = "dataSource")
    public void setDataSource(DataSource dataSource) {
        logger.info("setting data source in layers-store.layersDao");
        if (dataSource != null) {
            logger.info("dataSource is NOT null");
            logger.info(dataSource.toString());
        } else {
            logger.info("dataSource is null");
        }
        this.dataSource = dataSource;
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(this.jdbcTemplate);
        this.insertLayer = new SimpleJdbcInsert(dataSource).withTableName("layers").usingGeneratedKeyColumns("id");
    }

    @Override
    public List<Layer> getLayers() {
        logger.info("Getting a list of all enabled layers");
        String sql = "select * from layers where enabled=true";
        List<Layer> l = jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Layer.class));
        Util.updateDisplayPaths(l);
        Util.updateMetadataPaths(l);
        return l;
    }

    @Override
    public Layer getLayerById(int id) {
        return getLayerById(id, true);
    }

    @Override
    public Layer getLayerById(int id, boolean enabledLayersOnly) {
        logger.info("Getting enabled layer info for id = " + id);
        String sql = "select * from layers where id = ? ";
        if (enabledLayersOnly) {
            sql += " and enabled=true";
        }
        List<Layer> l = jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Layer.class), id);
        Util.updateDisplayPaths(l);
        Util.updateMetadataPaths(l);
        if (l.size() > 0) {
            return l.get(0);
        } else {
            return null;
        }
    }

    @Override
    public Layer getLayerByName(String name) {
        return getLayerByName(name, true);
    }

    @Override
    public Layer getLayerByName(String name, boolean enabledLayersOnly) {
        logger.info("Getting enabled layer info for name = " + name);
        String sql = "select * from layers where name = ? ";
        if (enabledLayersOnly) {
            sql += " and enabled=true";
        }
        List<Layer> l = jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Layer.class), name);
        Util.updateDisplayPaths(l);
        Util.updateMetadataPaths(l);
        logger.info("Searching for " + name + ": Found " + l.size() + " records. ");
        if (l.size() > 0) {
            return l.get(0);
        } else {
            return null;
        }
    }

    @Override
    public Layer getLayerByDisplayName(String name) {
        logger.info("Getting enabled layer info for name = " + name);
        String sql = "select * from layers where enabled=true and displayname = ?";
        List<Layer> l = jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Layer.class), name);
        Util.updateDisplayPaths(l);
        Util.updateMetadataPaths(l);
        if (l.size() > 0) {
            return l.get(0);
        } else {
            return null;
        }
    }

    @Override
    public List<Layer> getLayersByEnvironment() {
        String type = "Environmental";
        logger.info("Getting a list of all enabled environmental layers");
        String sql = "select * from layers where enabled=true and type = ?";
        List<Layer> l = jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Layer.class), type);
        Util.updateDisplayPaths(l);
        Util.updateMetadataPaths(l);
        return l;
    }

    @Override
    public List<Layer> getLayersByContextual() {
        String type = "Contextual";
        logger.info("Getting a list of all enabled Contextual layers");
        String sql = "select * from layers where enabled=true and type = ?";
        List<Layer> l = jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Layer.class), type);
        Util.updateDisplayPaths(l);
        Util.updateMetadataPaths(l);
        return l;
    }

    @Override
    public List<Layer> getLayersByCriteria(String keywords) {
        logger.info("Getting a list of all enabled layers by criteria: " + keywords);
        String sql = "";
        sql += "select * from layers where ";
        sql += " enabled=true AND ( ";
        sql += "keywords ilike ? ";
        sql += " or displayname ilike ? ";

        sql += " or name ilike ? ";
        sql += " or domain ilike ? ";
        sql += ") order by displayname ";

        keywords = "%" + keywords + "%";

        List<Layer> list = jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Layer.class), keywords, keywords, keywords, keywords);

        //remove duplicates if any
        Set setItems = new LinkedHashSet(list);
        list.clear();
        list.addAll(setItems);

        Util.updateDisplayPaths(list);
        Util.updateMetadataPaths(list);

        return list;
    }

    @Override
    public Layer getLayerByIdForAdmin(int id) {
        logger.info("Getting enabled layer info for id = " + id);
        String sql = "select * from layers where id = ?";
        List<Layer> l = jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Layer.class), id);
        Util.updateDisplayPaths(l);
        Util.updateMetadataPaths(l);
        if (l.size() > 0) {
            return l.get(0);
        } else {
            return null;
        }
    }

    @Override
    public Layer getLayerByNameForAdmin(String name) {
        logger.info("Getting enabled layer info for name = " + name);
        String sql = "select * from layers where name = ?";
        List<Layer> l = jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Layer.class), name);
        Util.updateDisplayPaths(l);
        Util.updateMetadataPaths(l);
        if (l.size() > 0) {
            return l.get(0);
        } else {
            return null;
        }
    }

    @Override
    public List<Layer> getLayersForAdmin() {
        logger.info("Getting a list of all layers");
        String sql = "select * from layers";
        List<Layer> l = jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Layer.class));
        Util.updateDisplayPaths(l);
        Util.updateMetadataPaths(l);
        return l;
    }

    @Override
    public void addLayer(Layer layer) {
        logger.info("Add new layer metadta for " + layer.getName());

        Map<String, Object> parameters = layer.toMap();
        parameters.remove("uid");
        parameters.remove("id");
        insertLayer.execute(parameters);
        //layer.name is unique, fetch newId
        Layer newLayer = getLayerByName(layer.getName(), false);

        //attempt to apply requested layer id
        if (layer.getId() > 0 && getLayerById(layer.getId().intValue()) == null) {
            //requested id is not in use

            jdbcTemplate.update("UPDATE layers SET id=" + layer.getId() + " WHERE id=" + newLayer.getId());

            newLayer = getLayerByName(layer.getName(), false);
        }

        layer.setId(newLayer.getId());

    }

    @Override
    public void updateLayer(Layer layer) {
        logger.info("Updating layer metadata for " + layer.getName());
        String sql = "update layers set citation_date=:citation_date, classification1=:classification1, classification2=:classification2, datalang=:datalang, description=:description, displayname=:displayname, displaypath=:displaypath, enabled=:enabled, domain=:domain, environmentalvaluemax=:environmentalvaluemax, environmentalvaluemin=:environmentalvaluemin, environmentalvalueunits=:environmentalvalueunits, extents=:extents, keywords=:keywords, licence_link=:licence_link, licence_notes=:licence_notes, licence_level=:licence_level, lookuptablepath=:lookuptablepath, maxlatitude=:maxlatitude, maxlongitude=:maxlongitude, mddatest=:mddatest, mdhrlv=:mdhrlv, metadatapath=:metadatapath, minlatitude=:minlatitude, minlongitude=:minlongitude, name=:name, notes=:notes, path=:path, path_1km=:path_1km, path_250m=:path_250m, path_orig=:path_orig, pid=:pid, respparty_role=:respparty_role, scale=:scale, source=:source, source_link=:source_link, type=:type, uid=:uid where id=:id";
        namedParameterJdbcTemplate.update(sql, layer.toMap());
    }

    @Override
    public Connection getConnection() {
        if (connection == null) {
            try {
                connection = dataSource.getConnection();
            } catch (Exception e) {
                logger.error("failed to get datasource connection", e);
            }
        }
        return connection;
    }

    @Override
    public void delete(String layerId) {
        jdbcTemplate.update("delete from layers where id=" + Integer.parseInt(layerId));
    }
}
