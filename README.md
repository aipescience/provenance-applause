Provenance for the APPLAUSE archive
===================================

This script is a **prototype** for constructing provenance information for the APPLAUSE archive and can be used as an example to retrieve provenance information for a light curve, source or a plate. The provenance is written into a W3C provenance format document and stored as an png file as well. To get provenance of a light curve will take a very long time as each query is handled individually.

To connect to the database please use your login and password and insert those into the plate-archive.org.json file.
```
cp sample-plate-archive.org.json plate-archive.org.json
```

To connect to the database, the script relies on the uws-client:
```
pip install git+https://github.com/mtjvc/uws-client.git
```

Further packages:
pandas
numpy
prov
pydot
uws-client
