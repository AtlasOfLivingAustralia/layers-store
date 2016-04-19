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

import au.org.ala.layers.dto.Task;

import java.util.List;

/**
 * DAO for the Field object
 *
 * @author ajay
 */
public interface TaskDAO {
    List<Task> getTasks(int page, int pageSize, int minSize, int maxSize, boolean includeFinished, boolean includeStarted);

    void addTask(String name, String json, Integer size);

    boolean startTask(int id);

    void endTask(int id, String error);

    Task getNextTask(int maxSize);

    void resetStartedTasks();
}
