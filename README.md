Provenance for the APPLAUSE archive
===================================

This script is a prototype for constructing provenance information for the APPLAUSE archive. 

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