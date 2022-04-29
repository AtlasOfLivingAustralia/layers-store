### layers-store   [![Build Status](https://app.travis-ci.com/AtlasOfLivingAustralia/layers-store.svg?branch=master)](https://app.travis-ci.com/AtlasOfLivingAustralia/layers-store)

The layers store provides a DAO layer on top of the PostGIS database used to house the layers within a living atlas.
It includes various functions for:

* Tabulation generation - matrices of 2 contextual variables
* Point intersection - for Grid and Polygon data
* Legend generation
* Layer ingestion 

This library is referenced by the spatial-service grails module which provides a web service layer.

2.0.7-SNAPSHOT -> support Grails 4
