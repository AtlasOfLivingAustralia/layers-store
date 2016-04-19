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

import java.util.List;

/**
 * DAO for the Field object
 *
 * @author ajay
 */
public interface FieldDAO {
    List<Field> getFields();

    List<Field> getFields(boolean enabledFieldsOnly);

    Field getFieldById(String id);

    Field getFieldById(String id, boolean enabledFieldsOnly);

    List<Field> getFieldsByDB();

    void addField(Field field);

    void updateField(Field field);

    void delete(String fieldId);

    public List<Field> getFieldsByCriteria(String keywords);

    public List<Layer> getLayersByCriteria(String keywords);
}
