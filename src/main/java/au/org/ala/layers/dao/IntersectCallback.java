package au.org.ala.layers.dao;

import au.org.ala.layers.dto.IntersectionFile;

public interface IntersectCallback {
    public void setLayersToSample(IntersectionFile[] layersToSample);

    public void setCurrentLayer(IntersectionFile layer);

    public void setCurrentLayerIdx(Integer layerIdx);

    public void progressMessage(String message);
}
