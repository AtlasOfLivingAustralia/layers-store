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

import au.org.ala.layers.dto.Ud_header;
import au.org.ala.layers.legend.QueryField;

import java.util.List;
import java.util.Map;

public interface UserDataDAO {

    Ud_header put(String user_id, String record_type, String desc, String metadata, String data_path, String analysis_id);

    Ud_header update(Long ud_header_id, String user_id, String record_type, String description, String metadata, String data_path, String analysis_id);

    public Ud_header get(Long ud_header_id);

    public boolean delete(Long ud_header_id);

    public String[] getStringArray(String header_id, String ref);

    public boolean[] getBooleanArray(String header_id, String ref);

    public double[][] getDoublesArray(String header_id, String ref);

    public boolean setStringArray(String header_id, String ref, String[] data);

    public boolean setBooleanArray(String header_id, String ref, boolean[] data);

    public boolean setDoublesArray(String header_id, String ref, double[][] data);

    public List<Ud_header> list(String user_id);

    public boolean setDoubleArray(String ud_header_id, String ref, double[] points);

    public boolean setQueryField(String ud_header_id, String ref, QueryField qf);

    public double[] getDoubleArray(String ud_header_id, String ref);

    public QueryField getQueryField(String ud_header_id, String ref);

    public boolean setMetadata(long ud_header_id, Map metadata);

    public Map getMetadata(long ud_header_id);

    public List<String> listData(String ud_header_id, String data_type);

    public Ud_header facet(String ud_header_id, List<String> new_facets, String new_wkt);

    public String getSampleZip(String id, String fields);

    /**
     * Search user data headers.
     * <p>
     * Search terms are optional. Search term use: desc AND data_type AND (user_id OR data_path OR analysis_id)
     *
     * @param desc        String to limit description field with ilike, or null
     * @param data_type   String to limit with data_type, or null
     * @param user_id     String to match user_id, or null.
     * @param data_path   String to match data_path, or null.
     * @param analysis_id String to match analysis_id, or null.
     * @param start       Integer for paging start.
     * @param limit       Integer for number of records to return.
     * @return List of user data headers using the search; desc AND data_type AND (user_id OR data_path OR analysis_id)
     */
    public List<Ud_header> searchDescAndTypeOr(String desc, String data_type, String user_id, String data_path,
                                               String analysis_id, int start, int limit);
}
