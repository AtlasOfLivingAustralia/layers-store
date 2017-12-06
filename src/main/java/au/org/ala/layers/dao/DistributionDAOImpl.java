/**************************************************************************
 * Copyright (C) 2012 Atlas of Living Australia
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

import au.org.ala.layers.dto.Distribution;
import au.org.ala.layers.dto.Facet;
import au.org.ala.layers.intersect.IntersectConfig;
import au.org.ala.layers.util.Util;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.util.*;

/**
 * @author ajay
 */
@Service("distributionDao")
public class DistributionDAOImpl implements DistributionDAO {

    /**
     * log4j logger
     */
    private static final Logger logger = Logger.getLogger(DistributionDAOImpl.class);
    private final String SELECT_CLAUSE = "select gid,spcode,scientific,authority_,common_nam,\"family\",genus_name,specific_n,min_depth,"
            + "max_depth,pelagic_fl,coastal_fl,desmersal_fl,estuarine_fl,family_lsid,genus_lsid,caab_species_number,"
            + "caab_family_number,group_name,metadata_u,wmsurl,lsid,type,area_name,pid,checklist_name,area_km,notes,"
            + "geom_idx,image_quality,data_resource_uid,endemic";
    private JdbcTemplate jdbcTemplate;
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private String viewName = "distributions";
    private DataSource dataSource;

    public Distribution findDistributionByLSIDOrName(String lsidOrName, String type) {
        List<Distribution> ds = findDistributionsByLSIDOrName(lsidOrName, type);
        if (ds != null && ds.size() > 0) {
            return ds.get(0);
        } else {
            return null;
        }
    }

    public List<Distribution> findDistributionsByLSIDOrName(String lsidOrName, String type) {
        String sql = SELECT_CLAUSE + " from " + viewName + " WHERE " +
                "(lsid=:lsid OR caab_species_number=:caab_species_number " +
                "OR scientific like :scientificName OR scientific like :scientificNameWithSubgenus) AND type = :distribution_type limit 1";
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("lsid", lsidOrName);
        params.put("scientificName", lsidOrName);
        params.put("scientificNameWithSubgenus", removeSubGenus(lsidOrName));
        params.put("caab_species_number", lsidOrName);
        params.put("distribution_type", type);
        List<Distribution> ds = updateWMSUrl(namedParameterJdbcTemplate.query(sql, params, BeanPropertyRowMapper.newInstance(Distribution.class)));
        return ds;
    }

    private String removeSubGenus(String str) {
        if (str != null && str.contains("(") && str.contains(")"))
            return str.replaceAll(" \\([A-Z][a-z]{1,}\\) ", " ");
        return str;
    }

    @Override
    public List<Distribution> queryDistributions(String wkt, double min_depth, double max_depth, Integer geomIdx, String lsids, String type, String[] dataResources, Boolean endemic) {
        return queryDistributions(wkt, min_depth, max_depth, null, null, null, null, null, geomIdx, lsids, null, null, null, null, type, dataResources, endemic);
    }

    @Override
    public List<Distribution> queryDistributions(String wkt, double min_depth, double max_depth, Boolean pelagic,
                                                 Boolean coastal, Boolean estuarine, Boolean desmersal, String groupName,
                                                 Integer geomIdx, String lsids, String[] families, String[] familyLsids, String[] genera,
                                                 String[] generaLsids, String type, String[] dataResources, Boolean endemic) {
        logger.info("Getting distributions list");

        StringBuilder whereClause = new StringBuilder();
        Map<String, Object> params = new HashMap<String, Object>();
        constructWhereClause(min_depth, max_depth, pelagic, coastal, estuarine, desmersal, groupName, geomIdx, lsids,
                families, familyLsids, genera, generaLsids, type, dataResources, params, whereClause, endemic);
        if (wkt != null && wkt.length() > 0) {
            if (whereClause.length() > 0) {
                whereClause.append(" AND ");
            }
            whereClause.append("ST_INTERSECTS(the_geom, ST_GEOMFROMTEXT( :wkt , 4326))");
            params.put("wkt", wkt);
        }

        String sql = SELECT_CLAUSE + " from " + viewName;
        if (whereClause.length() > 0) {
            sql += " WHERE " + whereClause.toString();
        }

        return updateWMSUrl(namedParameterJdbcTemplate.query(sql, params, BeanPropertyRowMapper.newInstance(Distribution.class)));
    }

    @Override
    public List<Facet> queryDistributionsFamilyCounts(String wkt, double min_depth, double max_depth, Boolean pelagic, Boolean coastal, Boolean estuarine, Boolean desmersal, String groupName,
                                                      Integer geomIdx, String lsids, String[] families, String[] familyLsids, String[] genera, String[] generaLsids, String type, String[] dataResources, Boolean endemic) {
        logger.info("Getting distributions list - family counts");

        StringBuilder whereClause = new StringBuilder();
        Map<String, Object> params = new HashMap<String, Object>();
        constructWhereClause(min_depth, max_depth, pelagic, coastal, estuarine, desmersal, groupName, geomIdx, lsids,
                families, familyLsids, genera, generaLsids, type, dataResources, params, whereClause, endemic);
        if (wkt != null && wkt.length() > 0) {
            if (whereClause.length() > 0) {
                whereClause.append(" AND ");
            }
            whereClause.append("ST_INTERSECTS(the_geom, ST_GEOMFROMTEXT( :wkt , 4326))");
            params.put("wkt", wkt);
        }

        String sql = "Select family as name, count(*) as count from " + viewName;
        if (whereClause.length() > 0) {
            sql += " WHERE " + whereClause.toString();
        }
        sql = sql + " group by family";

        return namedParameterJdbcTemplate.query(sql, params, BeanPropertyRowMapper.newInstance(Facet.class));
    }


    @Override
    public Distribution getDistributionBySpcode(long spcode, String type, boolean noWkt) {
        String wktTerm = noWkt ? "" : ", ST_AsText(the_geom) AS geometry";
        String sql = SELECT_CLAUSE + wktTerm + ", ST_AsText(bounding_box) as bounding_box FROM " + viewName + " WHERE spcode= ? AND type= ?";
        List<Distribution> d = updateWMSUrl(jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Distribution.class), (double) spcode, type));
        if (d.size() > 0) {
            return d.get(0);
        }
        return null;
    }

    public List<Distribution> queryDistributionsByRadius(float longitude, float latitude, float radiusInMetres,
                                                         double min_depth, double max_depth, Integer geomIdx, String lsids, String[] families,
                                                         String[] familyLsids, String[] genera, String[] generaLsids, String type, String[] dataResources, Boolean endemic) {
        return queryDistributionsByRadius(longitude, latitude, radiusInMetres, min_depth, max_depth, null, null, null, null, null,
                geomIdx, lsids, families, familyLsids, genera, generaLsids, type, dataResources, endemic);
    }

    /**
     * Query by radius
     *
     * @return set of species with distributions intersecting the radius
     */
    public List<Distribution> queryDistributionsByRadius(float longitude, float latitude, float radiusInMetres, double min_depth, double max_depth, Boolean pelagic, Boolean coastal,
                                                         Boolean estuarine, Boolean desmersal, String groupName, Integer geomIdx, String lsids,
                                                         String[] families, String[] familyLsids, String[] genera, String[] generaLsids, String type, String[] dataResources, Boolean endemic) {
        logger.info("Getting distributions list with a radius - " + radiusInMetres + "m");

        Map<String, Object> params = new HashMap<String, Object>();
        params.put("radius", convertMetresToDecimalDegrees(radiusInMetres));
        params.put("type", type);
        String pointGeom = "POINT(" + longitude + " " + latitude + ")";

        String sql = SELECT_CLAUSE + " from " + viewName + " where ST_DWithin(the_geom, ST_GeomFromText('" + pointGeom + "', 4326), :radius)";
        // add additional criteria
        StringBuilder whereClause = new StringBuilder();

        constructWhereClause(min_depth, max_depth, pelagic, coastal, estuarine, desmersal, groupName, geomIdx, lsids,
                families, familyLsids, genera, generaLsids, type, dataResources, params, whereClause, endemic);

        if (whereClause.length() > 0) {
            sql += " AND " + whereClause.toString();
        }
        return updateWMSUrl(jdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(Distribution.class), params));
    }

    /**
     * Query by radius
     *
     * @return set of species with distributions intersecting the radius
     */
    public List<Facet> queryDistributionsByRadiusFamilyCounts(float longitude, float latitude, float radiusInMetres, double min_depth, double max_depth, Boolean pelagic, Boolean coastal,
                                                              Boolean estuarine, Boolean desmersal, String groupName, Integer geomIdx, String lsids, String[] families, String[] familyLsids,
                                                              String[] genera, String[] generaLsids, String type, String[] dataResources, Boolean endemic) {
        logger.info("Getting distributions list with a radius");

        Map<String, Object> params = new HashMap<String, Object>();
        params.put("radius", convertMetresToDecimalDegrees(radiusInMetres));
        params.put("type", type);
        String pointGeom = "POINT(" + longitude + " " + latitude + ")";

        String sql = "Select family as name, count(*) as count from " + viewName + " where ST_DWithin(the_geom, ST_GeomFromText('" + pointGeom + "', 4326), :radius)";

        // add additional criteria
        StringBuilder whereClause = new StringBuilder();

        constructWhereClause(min_depth, max_depth, pelagic, coastal, estuarine, desmersal, groupName, geomIdx, lsids,
                families, familyLsids, genera, generaLsids, type, dataResources, params, whereClause, endemic);

        if (whereClause.length() > 0) {
            sql += " AND " + whereClause.toString();
        }

        sql = sql + " group by family";

        return namedParameterJdbcTemplate.query(sql, params, BeanPropertyRowMapper.newInstance(Facet.class));
    }


    @Override
    public List<Distribution> getDistributionByLSID(String[] lsids, String type, boolean noWkt) {
        String wktTerm = noWkt ? "" : ", ST_AsText(the_geom) AS geometry";
        String sql = SELECT_CLAUSE + wktTerm + ", ST_AsText(bounding_box) as bounding_box FROM "
                + viewName + "  WHERE lsid IN (:lsids) AND type = :distribution_type ";
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("lsids", Arrays.asList(lsids));
        params.put("distribution_type", type);
        return updateWMSUrl(namedParameterJdbcTemplate.query(sql, params, BeanPropertyRowMapper.newInstance(Distribution.class)));
    }

    /**
     * WARNING: This conversion isnt accurate..
     *
     * @return
     */
    public Double convertMetresToDecimalDegrees(Float metres) {
        //0.01 degrees is approximately 1110 metres
        //0.00001 1.11 m
        return (metres / 1.11) * 0.00001;
    }

    /**
     * @param min_depth
     * @param max_depth
     * @param geomIdx
     * @param lsids
     * @param params
     * @param where
     */
    private void constructWhereClause(double min_depth, double max_depth, Boolean pelagic, Boolean coastal,
                                      Boolean estuarine, Boolean desmersal, String groupName, Integer geomIdx,
                                      String lsids, String[] families, String[] familyLsids, String[] genera,
                                      String[] generaLsids, String type, String[] dataResources, Map<String, Object> params,
                                      StringBuilder where, Boolean endemic) {
        if (geomIdx != null && geomIdx >= 0) {
            where.append(" geom_idx = :geom_idx ");
            params.put("geom_idx", geomIdx);
        }

        if (lsids != null && lsids.length() > 0) {
            if (where.length() > 0) {
                where.append(" AND ");
            }
            where.append(":lsids LIKE '% '||lsid||' %'  ");
            params.put("lsids", " " + lsids.replace(",", " ") + " ");
        }

        if (dataResources != null && dataResources.length > 0) {
            if (where.length() > 0) {
                where.append(" AND ");
            }
            where.append("data_resource_uid IN (:dataResources) ");
            params.put("dataResources", Arrays.asList(dataResources));
        }

        if (min_depth != -1 && max_depth != -1) {
            if (where.length() > 0) {
                where.append(" AND ");
            }
            where.append("min_depth <= :max_depth AND max_depth >= :min_depth ");
            params.put("max_depth", new Double(max_depth));
            params.put("min_depth", new Double(min_depth));
        } else if (min_depth != -1) {
            if (where.length() > 0) {
                where.append(" AND ");
            }
            where.append("max_depth >= :min_depth ");
            params.put("min_depth", new Double(min_depth));
        } else if (max_depth != -1) {
            if (where.length() > 0) {
                where.append(" AND ");
            }
            where.append("min_depth <= :max_depth ");
            params.put("max_depth", new Double(max_depth));
        }

        if (pelagic != null) {
            if (where.length() > 0) {
                where.append(" AND ");
            }
            if (pelagic) {
                where.append("pelagic_fl > 0 ");
            } else {
                where.append("pelagic_fl = 0 ");
            }
        }

        if (coastal != null) {
            if (where.length() > 0) {
                where.append(" AND ");
            }
            where.append("coastal_fl = :coastal ");
            params.put("coastal", coastal);// ? 1 : 0);
        }

        if (estuarine != null) {
            if (where.length() > 0) {
                where.append(" AND ");
            }
            where.append("estuarine_fl = :estuarine ");
            params.put("estuarine", estuarine);// ? 1 : 0);
        }

        if (desmersal != null) {
            if (where.length() > 0) {
                where.append(" AND ");
            }
            where.append("desmersal_fl = :desmersal ");
            params.put("desmersal", desmersal);// ? 1 : 0);
        }

        if (type != null) {
            if (where.length() > 0) {
                where.append(" AND ");
            }
            where.append("type = :distribution_type ");
            params.put("distribution_type", type);
        }

        if (groupName != null) {
            if (where.length() > 0) {
                where.append(" AND ");
            }
            where.append("group_name = :groupName ");
            params.put("groupName", groupName);
        }

        if (families != null) {
            if (where.length() > 0) {
                where.append(" AND ");
            }
            where.append("family IN (:families) ");
            params.put("families", Arrays.asList(families));
        }

        if (familyLsids != null) {
            if (where.length() > 0) {
                where.append(" AND ");
            }
            where.append("family_lsid IN (:familyLsids) ");
            params.put("familyLsids", Arrays.asList(familyLsids));
        }

        if (genera != null) {
            if (where.length() > 0) {
                where.append(" AND ");
            }
            where.append("genus_name IN (:genera) ");
            params.put("genera", Arrays.asList(genera));
        }

        if (generaLsids != null) {
            if (where.length() > 0) {
                where.append(" AND ");
            }
            where.append("genus_lsid IN (:generaLsids) ");
            params.put("generaLsids", Arrays.asList(generaLsids));
        }

        if (endemic != null) {
            if (where.length() > 0) {
                where.append(" AND ");
            }
            where.append("endemic= :endemic");
            params.put("endemic", endemic);
        }

    }

    private List<Distribution> updateWMSUrl(List<Distribution> distributions) {
        if (distributions != null) {
            for (Distribution distribution : distributions) {
                if (distribution.getWmsurl() != null) {
                    if (!distribution.getWmsurl().startsWith("/")) {
                        distribution.setWmsurl(distribution.getWmsurl().replace(IntersectConfig.GEOSERVER_URL_PLACEHOLDER, IntersectConfig.getGeoserverUrl()));
                    } else {
                        distribution.setWmsurl(IntersectConfig.getGeoserverUrl() + distribution.getWmsurl());
                    }
                }
            }
        }
        return distributions;
    }

    @Override
    public int getNumberOfVertices(String lsid, String type) {
        return jdbcTemplate.queryForObject("SELECT st_npoints(st_collect(ds.the_geom)) from distributionshapes ds " +
                "join distributiondata dd on dd.geom_idx = ds.id where dd.lsid=? and type=?", Integer.class, lsid, type);
    }

    @Override
    public void store(Distribution d, String source_url) {

        //distributionshapes
        //create if does not exist
        String sql = "select id from distributionshapes where id = " + d.getGeom_idx();
        List list = jdbcTemplate.queryForList(sql);
        if (list == null || list.size() == 0) {
            //fetch wkt if missing
            if (d.getGeometry() == null || d.getGeometry().length() == 0) {
                JSONObject jo = JSONObject.fromObject(Util.readUrl(source_url));
                d.setGeometry(jo.getString("geometry"));
            }

            String insertSql = "insert into distributionshapes (id, pid, the_geom, name, area_km) values ( ? , ? , st_geomfromtext(?, 4326) , ? , ?);";
            jdbcTemplate.update(insertSql, d.getGeom_idx(), d.getPid(), d.getGeometry(), d.getArea_name(), d.getArea_km());
        }

        //distributiondata
        //create if does not exist
        sql = "select spcode from distributiondata where spcode = " + d.getSpcode();
        list = jdbcTemplate.queryForList(sql);
        if (list == null || list.size() == 0) {

            String insertSql = "INSERT INTO public.distributiondata(" +
                    "            gid, spcode, scientific, authority_, common_nam, family, genus_name, " +
                    "            specific_n, min_depth, max_depth, pelagic_fl, metadata_u, the_geom, " +
                    "            wmsurl, lsid, geom_idx, type, checklist_name, notes, estuarine_fl, " +
                    "            coastal_fl, desmersal_fl, group_name, genus_exemplar, family_exemplar, " +
                    "            caab_species_number, caab_species_url, caab_family_number, caab_family_url, " +
                    "            metadata_uuid, family_lsid, genus_lsid, bounding_box, data_resource_uid, " +
                    "            original_scientific_name, image_quality, the_geom_orig, endemic)" +
                    "    VALUES (?, ?, ?, ?, ?, ?, ?, " +
                    "            ?, ?, ?, ?, ?, ?, " +
                    "            ?, ?, ?, ?, ?, ?, ?, " +
                    "            ?, ?, ?, ?, ?, " +
                    "            ?, ?, ?, ?, " +
                    "            ?, ?, ?, ?, ?, " +
                    "            ?, ?, ?, ?);";
            jdbcTemplate.update(insertSql,
                    d.getGid(), d.getSpcode(), d.getScientific(), d.getAuthority_(), d.getCommon_nam(), d.getFamily(), d.getGenus_name(),
                    d.getSpecific_n(), d.getMin_depth(), d.getMax_depth(), d.getPelagic_fl(), d.getMetadata_u(), null,
                    d.getWmsurl().substring(d.getWmsurl().indexOf("/wms?service")), d.getLsid(), d.getGeom_idx(), d.getType(), d.getChecklist_name(), d.getNotes(), d.getEstuarine_fl(),
                    d.getCoastal_fl(), d.getDesmersal_fl(), d.getGroup_name(), null, null,
                    d.getCaab_species_number(), null, d.getCaab_family_number(), null,
                    null, d.getFamily_lsid(), d.getGenus_lsid(), d.getBounding_box(), d.getData_resource_uid(),
                    null, d.getImage_quality(), null, d.getEndemic());
        }
    }

    @Override
    public Map<String, Double> identifyOutlierPointsForDistribution(String lsid, Map<String, Map<String, Double>> points, String type) {
        Map<String, Double> outlierDistances = new HashMap<String, Double>();
        Map<String, String> uuidLookup = new HashMap<String, String>();

        try {
            StringBuilder pointsString = new StringBuilder();
            List<String> uuids = new ArrayList<String>();
            for (String uuid : points.keySet()) {
                Map<String, Double> pointDetails = points.get(uuid);
                if (pointDetails != null) {
                    Double latitude = pointDetails.get("decimalLatitude");
                    Double longitude = pointDetails.get("decimalLongitude");
                    if (latitude != null && longitude != null) {
                        if (pointsString.length() > 0) {
                            pointsString.append(",");
                        } else {
                            pointsString.append("MULTIPOINT(");
                        }
                        pointsString.append(longitude).append(" ").append(latitude);
                        uuids.add(uuid);
                        uuidLookup.put(longitude + " " + latitude, uuid);
                    }
                }
            }
            pointsString.append(")");
            List<Map<String, Object>> outlierDistancesQueryResult = jdbcTemplate.queryForList(
                    "select points.path as id, st_x(points.geom) as x, st_y(points.geom) as y," +
                            " ST_DISTANCE(points.geom, d.the_geom) as distance from " +
                            "(select geography(st_collect(the_geom)) as the_geom, st_setsrid(st_extent(bounding_box), 4326) as bounding_box " +
                            "from distributions where " +
                            "lsid = ? and type = ? ) d, " +
                            "st_dump(st_setsrid(sT_GeomFromText( ? ), 4326)) points " +
                            "where bounding_box is null or st_intersects(d.bounding_box, points.geom) ",
                    lsid, type, pointsString);

            for (Map<String, Object> queryResultRow : outlierDistancesQueryResult) {
                Double distance = (Double) queryResultRow.get("distance");
                // Zero distance implies that the point is inside the
                // distribution
                if (distance > 0) {
                    String id = queryResultRow.get("id").toString();

                    //not sure why st_dump .path is sometimes null
                    if (id != null) {
                        id = uuids.get(Integer.parseInt(queryResultRow.get("id").toString().replace("{", "").replace("}", "")) - 1);
                    } else {
                        //x and y as double for comparisons
                        String key = queryResultRow.get("x") + " " + queryResultRow.get("y");
                        id = uuidLookup.get(key);

                        //handle duplicate points
                        uuidLookup.remove(key);
                    }

                    if (id == null) {
                        logger.error("Error fetching uuid for distribution distance with xy: " + queryResultRow.get("x") + " " + queryResultRow.get("y"));
                    }
                    outlierDistances.put(id, distance);
                }
            }
        } catch (EmptyResultDataAccessException ex) {
            throw new IllegalArgumentException("No expert distribution associated with lsid " + lsid, ex);
        }

        return outlierDistances;
    }

    @Resource(name = "dataSource")
    public void setDataSource(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(this.jdbcTemplate);
        this.dataSource = dataSource;
    }

    public void setViewName(String viewName) {
        this.viewName = viewName;
    }
}
