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

import au.org.ala.layers.dto.Objects;
import au.org.ala.layers.util.LayerFilter;
import org.springframework.scheduling.annotation.Async;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 * DAO for the Object object
 *
 * @author ajay
 */
public interface ObjectDAO {

    List<Objects> getObjects();

    List<Objects> getObjectsById(String id);

    List<Objects> getObjectsById(String id, int start, int pageSize);

    List<Objects> getObjectsById(String id, int start, int pageSize, String filter);

    void writeObjectsToCSV(OutputStream output, String id) throws Exception;

    String getObjectsGeometryById(String id, String geomtype);

    Objects getObjectByPid(String pid);

    Objects getObjectByIdAndLocation(String fid, Double lng, Double lat);

    List<Objects> getNearestObjectByIdAndLocation(String fid, int limit, Double lng, Double lat);

    void streamObjectsGeometryById(OutputStream os, String id, String geomtype) throws IOException;

    List<Objects> getObjectByFidAndName(String fid, String name);

    List<Objects> getObjectsByIdAndArea(String id, Integer limit, String wkt);

    List<Objects> getObjectsByIdAndIntersection(String id, Integer limit, LayerFilter layerFilter);

    List<Objects> getObjectsByIdAndIntersection(String id, Integer limit, String intersectingPid);

    String createUserUploadedObject(String wkt, String name, String description, String userid);

    String createUserUploadedObject(String wkt, String name, String description, String userid, boolean namesearch);

    boolean updateUserUploadedObject(int pid, String wkt, String name, String description, String userid);

    boolean deleteUserUploadedObject(int pid);

    int createPointOfInterest(String objectId, String name, String type, Double latitude, Double longitude, Double bearing, String userId, String description, Double focalLength);

    boolean updatePointOfInterest(int id, String objectId, String name, String type, Double latitude, Double longitude, Double bearing, String userId, String description, Double focalLength);

    boolean deletePointOfInterest(int id);

    Map<String, Object> getPointOfInterestDetails(int id);

    List<Objects> getObjectsWithinRadius(String fid, double latitude, double longitude, double radiusKm);

    List<Objects> getObjectsIntersectingWithGeometry(String fid, String wkt);

    List<Objects> getObjectsIntersectingWithObject(String fid, String objectPid);

    List<Map<String, Object>> getPointsOfInterestWithinRadius(double latitude, double longitude, double radiusKm);

    List<Map<String, Object>> pointsOfInterestGeometryIntersect(String wkt);

    List<Map<String, Object>> pointsOfInterestObjectIntersect(String objectPid);

    int getPointsOfInterestWithinRadiusCount(double latitude, double longitude, double radiusKm);

    int pointsOfInterestGeometryIntersectCount(String wkt);

    int pointsOfInterestObjectIntersectCount(String objectPid);

    Objects intersectObject(String pid, double latitude, double longitude);

    @Async
    void updateObjectNames();
}
