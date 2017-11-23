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
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.sql.DataSource;
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
    @Resource(name = "layerIntersectDao")
    private LayerIntersectDAO layerIntersectDao;

    @Resource(name = "dataSource")
    public void setDataSource(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public List<SearchObject> findByCriteria(final String criteria, int limit) {
        logger.info("Getting search results for query: " + criteria);
        String sql = "select pid, id, name, \"desc\" as description, fid, fieldname from searchobjects(?,?)";
        return addGridClassesToSearch(jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(SearchObject.class), "%" + criteria + "%", limit), criteria, limit);

    }

    private List<SearchObject> addGridClassesToSearch(List<SearchObject> search, String criteria, int limit) {
        criteria = criteria.toLowerCase();
        int maxPos = Integer.MAX_VALUE;
        int pos;
        for (SearchObject so : search) {
            pos = so.getName().toLowerCase().indexOf(criteria);
            if (pos >= 0 && pos < maxPos) {
                maxPos = pos;
            }
        }
        for (Entry<String, IntersectionFile> e : layerIntersectDao.getConfig().getIntersectionFiles().entrySet()) {
            IntersectionFile f = e.getValue();
            if ("a".equalsIgnoreCase(f.getType()) && f.getClasses() != null && e.getKey().equals(f.getFieldId())) {
                //search
                for (Entry<Integer, GridClass> c : f.getClasses().entrySet()) {
                    if ((pos = c.getValue().getName().toLowerCase().indexOf(criteria)) >= 0) {
                        if (pos <= maxPos) {
                            search.add(SearchObject.create(
                                    f.getLayerPid() + ":" + c.getKey(),
                                    f.getLayerPid() + ":" + c.getKey(),
                                    c.getValue().getName(),
                                    null,
                                    f.getFieldId(),
                                    f.getFieldName()));
                        }
                    }
                }
            }
        }
        return search;
    }
}
