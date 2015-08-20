debs-data-publisher
===================

Data publisher for DEBS 2014 usecase, compatible with DAS 3.0.0 onwards

###Usage

 - Building project from source

  ```$ mvn clean install```

 - Publishing data

  ```$ ./publish.sh [1] [2] [3] [4] [5] [6]```
  ```
  [1] - server url
  [2] - username
  [3] - password
  [4] - file location
  [5] - count (# of records in file to be published). -1 for ALL records
  [6] - # of publishing jobs to run (publishers to be spawned)
  ```

Example usage: ./publish.sh 192.168.19.1:7611 admin admin /opt/data/dataset.csv 10000 5
