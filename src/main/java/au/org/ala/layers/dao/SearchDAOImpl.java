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
import au.org.ala.layers.dto.SearchObject;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

/**
 * @author ajay
 */
@Service("searchDao")
public class SearchDAOImpl implements SearchDAO {

    /**
     * log4j logger
     */
    private static final Logger logger = Logger.getLogger(SearchDAOImpl.class);
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
    public List<SearchObject> findByCriteria(final String criteria, int limit) {
        logger.info("Getting search results for query: " + criteria);
        String sql = "select pid, id, name, \"desc\" as description, fid, fieldname from searchobjects(?,?)";
        return addGridClassesToSearch(jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(SearchObject.class), "%" + criteria + "%", limit), criteria, limit, null, null);
    }

    @Override
    public List<SearchObject> findByCriteria(String criteria, int offset, int limit) {
        return findByCriteria( criteria,  offset,  limit, new ArrayList<String>(), new ArrayList<String>());
    }

    @Override
    public List<SearchObject> findByCriteria(final String criteria, int limit, List<String> includeFieldIds, List<String> excludeFieldIds) {
       return findByCriteria(criteria, 0, limit, includeFieldIds,  excludeFieldIds);
    }

    @Override
    public List<SearchObject> findByCriteria(String criteria, int offset, int limit, List<String> includeFieldIds, List<String> excludeFieldIds) {
        logger.info("Getting search results for query: " + criteria);
        String fieldFilter = "";
        List<String> fieldIds = null;
        if (!includeFieldIds.isEmpty()) {
            fieldFilter = " and o.fid in ( :fieldIds ) ";
            fieldIds = includeFieldIds;
        } else if (!excludeFieldIds.isEmpty()) {
            fieldFilter = " and o.fid not in ( :fieldIds ) ";
            fieldIds = excludeFieldIds;
        }


        if (fieldFilter.isEmpty()) {
            // no fieldFilter
            //String sql = "select o.pid as pid ,o.id as id, o.name as name, o.desc as description, o.fid as fid, f.name as fieldname from objects o inner join fields f on o.fid = f.id where o.name ilike :criteria and o.namesearch=true order by position(:nativeQ in lower(o.name)), pid limit :limit offset :offset";
            String sql = "with o as (select o.pid as pid ,o.id as id, o.name as name, o.desc as description, o.fid as fid, f.name as fieldname from objects o inner join fields f on o.fid = f.id where o.name ilike :criteria and o.namesearch=true )" +
                    " select pid, id, name, description, fid, fieldname, (select json_agg(a.f) from (select distinct (fid || '|' || fieldname) as f from o) a) as fields, position(:nativeQ in lower(name)) as rank from o order by rank, name, pid limit :limit offset :offset";

            MapSqlParameterSource parameters = new MapSqlParameterSource();
            parameters.addValue("nativeQ",  criteria );
            parameters.addValue("criteria", "%" + criteria + "%");
            parameters.addValue("limit", limit);
            parameters.addValue("offset", offset);
            List<SearchObject> searchObjects = jdbcParameterTemplate.query(sql, parameters, BeanPropertyRowMapper.newInstance(SearchObject.class));

            return addGridClassesToSearch(searchObjects, criteria,  limit, includeFieldIds, excludeFieldIds);
        } else {
            // use fieldFilter
            MapSqlParameterSource parameters = new MapSqlParameterSource();
            parameters.addValue("fieldIds", fieldIds);
            parameters.addValue("nativeQ",  criteria );
            parameters.addValue("criteria", "%" + criteria + "%");
            parameters.addValue("limit", limit);
            parameters.addValue("offset", offset);

            String sql = "with o as (select o.pid as pid ,o.id as id, o.name as name, o.desc as description, o.fid as fid, f.name as fieldname from objects o inner join fields f on o.fid = f.id where o.name ilike :criteria and o.namesearch=true " + fieldFilter + ")"+
                    " select pid, id, name, description, fid, fieldname, (select json_agg(a.f) from (select distinct (fid || '|' || fieldname) as f from o) a) as fields, position(:nativeQ in lower(name)) as rank from o order by rank, name, pid limit :limit offset :offset";

            return addGridClassesToSearch(jdbcParameterTemplate.query(sql, parameters, BeanPropertyRowMapper.newInstance(SearchObject.class)), criteria, limit, includeFieldIds, excludeFieldIds);
        }
    }


    private List<SearchObject> addGridClassesToSearch(List<SearchObject> search, String criteria, int limit, List<String> includeFieldIds, List<String> excludeFieldIds) {
        criteria = criteria.toLowerCase();
        int vacantCount = limit - search.size() ;

        if (vacantCount>0)
            for (Entry<String, IntersectionFile> e : layerIntersectDao.getConfig().getIntersectionFiles().entrySet()) {
                IntersectionFile f = e.getValue();
                if ("a".equalsIgnoreCase(f.getType()) && f.getClasses() != null && e.getKey().equals(f.getFieldId()) &&
                        (includeFieldIds == null || includeFieldIds.isEmpty() || includeFieldIds.contains(f.getFieldId())) &&
                        (excludeFieldIds == null || excludeFieldIds.isEmpty() || !excludeFieldIds.contains(f.getFieldId()))) {
                    //search
                    for (Entry<Integer, GridClass> c : f.getClasses().entrySet()) {
                        if ((c.getValue().getName().toLowerCase().indexOf(criteria)) >= 0 ) {
                              search.add(SearchObject.create(
                                     f.getLayerPid() + ":" + c.getKey(),
                                     f.getLayerPid() + ":" + c.getKey(),
                                     c.getValue().getName(),
                                     null,
                                     f.getFieldId(),
                                     f.getFieldName(), ""));

                        }else{
                            break;
                        }
                    }
                }
            }
        return search;
    }
}
