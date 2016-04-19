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
package au.org.ala.layers.dto;

import java.util.HashMap;

/**
 * @author Adam
 */
public class IntersectionFile {

    String name;
    String filePath;
    String shapeFields;
    String layerName;
    String fieldId;
    String layerPid;
    String fieldName;
    String type;
    HashMap<Integer, GridClass> classes;

    public IntersectionFile(String name, String filePath, String shapeFields, String layerName, String fieldId, String fieldName, String layerPid, String type, HashMap<Integer, GridClass> classes) {
        this.name = name.trim();
        this.filePath = filePath.trim();
        this.shapeFields = (shapeFields == null) ? null : shapeFields.trim();
        this.layerName = layerName;
        this.fieldId = fieldId;
        this.fieldName = fieldName;
        this.layerPid = layerPid;
        this.type = type;
        this.classes = classes;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getShapeFields() {
        return shapeFields;
    }

    public void setShapeFields(String shapeFields) {
        this.shapeFields = shapeFields;
    }

    public String getLayerName() {
        return layerName;
    }

    public void setLayerName(String layerName) {
        this.layerName = layerName;
    }

    public String getFieldId() {
        return fieldId;
    }

    public void setFieldId(String fieldId) {
        this.fieldId = fieldId;
    }

    public HashMap<Integer, GridClass> getClasses() {
        return classes;
    }

    public void setClasses(HashMap<Integer, GridClass> classes) {
        this.classes = classes;
    }

    public String getLayerPid() {
        return layerPid;
    }

    public void setLayerPid(String layerPid) {
        this.layerPid = layerPid;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
