debs-data-publisher
===================

WSO2 BAM data publisher for DEBS 2014 usecase

This publisher creates randomized data during runtime for publication.

###Usage

 - Building project from source

  ```$ mvn clean install```

 - Publishing data

  ```$ ./publish.sh [1] [2] [3] [4] [5]```
  ```
  [1] - server url
  [2] - username
  [3] - password
  [4] - count (# of records in file to be published). -1 for ALL records
  [5] - # of publishing jobs to run (publishers to be spawned)
  ```

Example usage: ./publish.sh 192.168.19.1:7611 admin admin 10000 5
