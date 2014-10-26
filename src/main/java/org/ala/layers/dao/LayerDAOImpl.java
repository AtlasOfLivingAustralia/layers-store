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
package org.ala.layers.dao;

import org.ala.layers.dto.Layer;
import org.ala.layers.intersect.IntersectConfig;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.simple.ParameterizedBeanPropertyRowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

/**
 * @author ajay
 */
@Service("layerDao")
public class LayerDAOImpl implements LayerDAO {

    /**
     * log4j logger
     */
    private static final Logger logger = Logger.getLogger(LayerDAOImpl.class);
    private SimpleJdbcTemplate jdbcTemplate;
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
        this.jdbcTemplate = new SimpleJdbcTemplate(dataSource);
        this.insertLayer = new SimpleJdbcInsert(dataSource).withTableName("layers").usingGeneratedKeyColumns("id");
    }

    @Override
    public List<Layer> getLayers() {
        //return hibernateTemplate.find("from Layer where enabled=true");
        logger.info("Getting a list of all enabled layers");
        String sql = "select * from layers where enabled=true";
        List<Layer> l = jdbcTemplate.query(sql, ParameterizedBeanPropertyRowMapper.newInstance(Layer.class));
        updateDisplayPaths(l);
        updateMetadataPaths(l);
        return l;
    }

    @Override
    public Layer getLayerById(int id) {
        return getLayerById(id, true);
    }

    @Override
    public Layer getLayerById(int id, boolean enabledLayersOnly) {
        //List<Layer> layers = hibernateTemplate.find("from Layer where enabled=true and id=?", id);
        logger.info("Getting enabled layer info for id = " + id);
        String sql = "select * from layers where id = ? ";
        if (enabledLayersOnly) {
            sql += " and enabled=true";
        }
        List<Layer> l = jdbcTemplate.query(sql, ParameterizedBeanPropertyRowMapper.newInstance(Layer.class), id);
        updateDisplayPaths(l);
        updateMetadataPaths(l);
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
        //List<Layer> layers = hibernateTemplate.find("from Layer where enabled=true and name=?", name);

        logger.info("Getting enabled layer info for name = " + name);
        String sql = "select * from layers where name = ? ";
        if (enabledLayersOnly) {
            sql += " and enabled=true";
        }
        List<Layer> l = jdbcTemplate.query(sql, ParameterizedBeanPropertyRowMapper.newInstance(Layer.class), name);
        updateDisplayPaths(l);
        updateMetadataPaths(l);
        logger.info("Searching for " + name + ": Found " + l.size() + " records. ");
        if (l.size() > 0) {
            return l.get(0);
        } else {
            return null;
        }
    }

    @Override
    public Layer getLayerByDisplayName(String name) {
        //List<Layer> layers = hibernateTemplate.find("from Layer where enabled=true and name=?", name);

        logger.info("Getting enabled layer info for name = " + name);
        String sql = "select * from layers where enabled=true and displayname = ?";
        List<Layer> l = jdbcTemplate.query(sql, ParameterizedBeanPropertyRowMapper.newInstance(Layer.class), name);
        updateDisplayPaths(l);
        updateMetadataPaths(l);
        if (l.size() > 0) {
            return l.get(0);
        } else {
            return null;
        }
    }

    @Override
    public List<Layer> getLayersByEnvironment() {
        //return hibernateTemplate.find("from Layer where enabled=true and type='Environmental'");
        String type = "Environmental";
        logger.info("Getting a list of all enabled environmental layers");
        String sql = "select * from layers where enabled=true and type = ?";
        List<Layer> l = jdbcTemplate.query(sql, ParameterizedBeanPropertyRowMapper.newInstance(Layer.class), type);
        updateDisplayPaths(l);
        updateMetadataPaths(l);
        return l;
    }

    @Override
    public List<Layer> getLayersByContextual() {
        //return hibernateTemplate.find("from Layer where enabled=true and type='Contextual'");
        String type = "Contextual";
        logger.info("Getting a list of all enabled Contextual layers");
        String sql = "select * from layers where enabled=true and type = ?";
        List<Layer> l = jdbcTemplate.query(sql, ParameterizedBeanPropertyRowMapper.newInstance(Layer.class), type);
        updateDisplayPaths(l);
        updateMetadataPaths(l);
        return l;
    }

    @Override
    public List<Layer> getLayersByCriteria(String keywords) {
        logger.info("Getting a list of all enabled layers by criteria: " + keywords);
        String sql = "";
        sql += "select * from layers where ";
        sql += " enabled=true AND ( ";
        sql += "lower(keywords) like ? ";
        sql += " or lower(displayname) like ? ";
        //sql += " or lower(type) like ? ";
        sql += " or lower(name) like ? ";
        sql += " or lower(domain) like ? ";
        sql += ") order by displayname ";

        keywords = "%" + keywords.toLowerCase() + "%";

        //List list = hibernateTemplate.find(sql, new String[]{keywords, keywords, keywords}); // keywords,
        List<Layer> list = jdbcTemplate.query(sql, ParameterizedBeanPropertyRowMapper.newInstance(Layer.class), keywords, keywords, keywords, keywords);

        //remove duplicates if any
        Set setItems = new LinkedHashSet(list);
        list.clear();
        list.addAll(setItems);

        updateDisplayPaths(list);
        updateMetadataPaths(list);

        return list;//no duplicates now
//        logger.info("Getting a list of all enabled layers");
//        String sql = "select * from layers where enabled=true";
//        List<Layer> l = jdbcTemplate.query(sql, ParameterizedBeanPropertyRowMapper.newInstance(Layer.class));
//        return l;

    }

    @Override
    public Layer getLayerByIdForAdmin(int id) {
        //List<Layer> layers = hibernateTemplate.find("from Layer where enabled=true and id=?", id);
        logger.info("Getting enabled layer info for id = " + id);
        String sql = "select * from layers where id = ?";
        List<Layer> l = jdbcTemplate.query(sql, ParameterizedBeanPropertyRowMapper.newInstance(Layer.class), id);
        updateDisplayPaths(l);
        updateMetadataPaths(l);
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
        List<Layer> l = jdbcTemplate.query(sql, ParameterizedBeanPropertyRowMapper.newInstance(Layer.class), name);
        updateDisplayPaths(l);
        updateMetadataPaths(l);
        if (l.size() > 0) {
            return l.get(0);
        } else {
            return null;
        }
    }

    @Override
    public List<Layer> getLayersForAdmin() {
        //return hibernateTemplate.find("from Layer where enabled=true");
        logger.info("Getting a list of all layers");
        String sql = "select * from layers";
        List<Layer> l = jdbcTemplate.query(sql, ParameterizedBeanPropertyRowMapper.newInstance(Layer.class));
        updateDisplayPaths(l);
        updateMetadataPaths(l);
        return l;
    }

    @Override
    public void addLayer(Layer layer) {
        logger.info("Add new layer metadta for " + layer.getName());
        String sql = "insert into layers (citation_date,classification1,classification2,datalang,description,displayname,displaypath,enabled,domain,environmentalvaluemax,environmentalvaluemin,environmentalvalueunits,extents,keywords,licence_link,licence_notes,licence_level,lookuptablepath,maxlatitude,maxlongitude,mddatest,mdhrlv,metadatapath,minlatitude,minlongitude,name,notes,path,path_1km,path_250m,path_orig,pid,respparty_role,scale,source,source_link,type) values (:citation_date,:classification1,:classification2,:datalang,:description,:displayname,:displaypath,:enabled,:domain,:environmentalvaluemax,:environmentalvaluemin,:environmentalvalueunits,:extents,:keywords,:licence_link,:licence_notes,:licence_level,:lookuptablepath,:maxlatitude,:maxlongitude,:mddatest,:mdhrlv,:metadatapath,:minlatitude,:minlongitude,:name,:notes,:path,:path_1km,:path_250m,:path_orig,:pid,:respparty_role,:scale,:source,:source_link,:type)";
        //jdbcTemplate.update(sql, layer.toMap());
        Map<String, Object> parameters = layer.toMap();
        parameters.remove("uid");
        parameters.remove("id");
        insertLayer.execute(parameters);
        //layer.name is unique, fetch newId
        Layer newLayer = getLayerByName(layer.getName(), false);
        layer.setId(newLayer.getId());
        layer.setUid(newLayer.getId() + "");
        //updateLayer(layer);
    }

    @Override
    public void updateLayer(Layer layer) {
        logger.info("Updating layer metadata for " + layer.getName());
        String sql = "update layers set citation_date=:citation_date, classification1=:classification1, classification2=:classification2, datalang=:datalang, description=:description, displayname=:displayname, displaypath=:displaypath, enabled=:enabled, domain=:domain, environmentalvaluemax=:environmentalvaluemax, environmentalvaluemin=:environmentalvaluemin, environmentalvalueunits=:environmentalvalueunits, extents=:extents, keywords=:keywords, licence_link=:licence_link, licence_notes=:licence_notes, licence_level=:licence_level, lookuptablepath=:lookuptablepath, maxlatitude=:maxlatitude, maxlongitude=:maxlongitude, mddatest=:mddatest, mdhrlv=:mdhrlv, metadatapath=:metadatapath, minlatitude=:minlatitude, minlongitude=:minlongitude, name=:name, notes=:notes, path=:path, path_1km=:path_1km, path_250m=:path_250m, path_orig=:path_orig, pid=:pid, respparty_role=:respparty_role, scale=:scale, source=:source, source_link=:source_link, type=:type, uid=:uid where id=:id";
        jdbcTemplate.update(sql, layer.toMap());
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

    private void updateDisplayPaths(List<Layer> layers) {
        if (layers == null) {
            return;
        }

        for (Layer layer : layers) {
            if (layer.getDisplaypath() != null) {
                if (!layer.getDisplaypath().startsWith("/")) {
                    layer.setDisplaypath(layer.getDisplaypath().replace(IntersectConfig.GEOSERVER_URL_PLACEHOLDER, IntersectConfig.getGeoserverUrl()));
                } else {
                    layer.setDisplaypath(IntersectConfig.getGeoserverUrl() + layer.getDisplaypath());
                }
            }
        }
    }

    private void updateMetadataPaths(List<Layer> layers) {
        if (layers == null) {
            return;
        }

        for (Layer layer : layers) {
            if (layer.getMetadatapath() != null) {
                if (!layer.getMetadatapath().startsWith("/")) {
                    layer.setMetadatapath(layer.getMetadatapath().replace(IntersectConfig.GEONETWORK_URL_PLACEHOLDER, IntersectConfig.getGeonetworkUrl()));
                } else {
                    layer.setMetadatapath(IntersectConfig.getGeonetworkUrl() + layer.getMetadatapath());
                }
            }
        }
    }
}
