# ETL_Job_Threading
ETL job using native python and Pandas in a threading way

This ETL job works in a smart way where each thread/worker simultaneously grabs chunks of data from a directory named ```input```, performs ETL and produces chunks of the **successful** processed data to  a directory named ```output``` and also to the ```archive``` directory, and in case of **failure** it get moved to the ```error``` directory.
The degree of parallelism is given on the command line as an option to the ```program (-p)``` and the default is set to 3.

The **input** directory is structured as follows (assume local UNIX file system): 
```
 input
   |	metadata
   ||	applicant_nationality.json
   ||	applicant_employer.json
   |
   |	checks
   ||	right_to_work
   |||	2017-07-26-05.csv
   |||	2017-07-26-06.csv
   ||	...
   |
   |	identity
   ||	2017-07-26-05.csv
   ||	2017-07-26-06.csv
   ||	...
```
Chunks of input data are represented by files from 'input/checks/right_to_work'and'input/checks/identity'.Each chunk is a file from each directory that has the same timestamp as part of the file name (in BST time zone), which will have granularity of one hour.

Records from 'input/checks/right_to_work' files are comma-separated in the following format:
    
    unix_timestamp,applicant_id,applicant_employer,applicant_nationality,is_eligble

Records from 'input/checks/identity' files are comma-separated in the following format:

    unix_timestamp,applicant_id,is_verified

The program will merge **right_to_work** and **identity** checks records with the same applicant_id within the same hour and produce a file to ```output``` for **each input hour** with the following format (JSON):

    {"iso8601_timestamp":string,"applicant_id":string,"applicant_employer":string,"applicant_nationality":s tring,"is_eligble":bool,"is_verified":bool}

Where the timestamp will be in ISO-8601 format (BST time zone) of the record in the 'right_to_work' files, and the applicant_employer and applicant_nationality fields will be the string representation found from the input metadata lookup files.

A complete example of input and output files follows these instructions at the end.

A separate **logs** dir will contain processing log files from program execution.
All errors/exceptions should be logged there. In addition, there should be a ```log line``` for **every input hour** read indicating start of processing, and ```another log line``` for that input hour when processing completes, which includes **elapsed time** to process that hour.

## Complete example of input/output files:


    • input/metadata/applicant_employer.json (list of id/name lists)
    [[1,"Uber"],[2,"Tesco"],[3,"ZipCar"],[4,"BlaBlaCar"],[5,"Deliveroo"]]

    • input/metadata/applicant_nationality.json (list of id/name lists)
    [[1,"British"],[2,"Polish"],[3,"French"],[4,"Belgian"],[5,"Turkish"]]

    • input/checks/right_to_work/2017-07-26-05.csv
    1501043799,1,2,1,true
    1501044312,3,3,2,false

    • input/checks/right_to_work/2017-07-26-06.csv
    1501045965,2,5,4,false
    1501045652,5,4,3,true
    1501048356,4,1,5,false

    • input/checks/identity/2017-07-26-05.csv
    1501043859,1,true

    • input/checks/identity/2017-07-26-06.csv
    1501045656,5,true
    1501045971,2,true

    • output/2017-07-26-05.json
    {"iso8601_timestamp":"2017-07-26T05:36:39","applicant_id":"1","applicant_employer":"Tesco","applicant_nationality":"British","is_eligble":true,"is_verified":true}
    {"iso8601_timestamp":"2017-07-26T05:45:12","applicant_id":"3","applicant_employer":"ZipCar","applicant_nationality":"Polish","is_eligble":false}

    • output/2017-07-26-06.json
    {"iso8601_timestamp":"2017-07-26T06:12:45","applicant_id":"2","applicant_employer":"Deliver oo","applicant_nationality":"Belgian","is_eligble":false,"is_verified":true}
    {"iso8601_timestamp":"2017-07-26T06:07:32","applicant_id":"5","applicant_employer":"BlaBlaCar","applicant_nationality":"French","is_eligble":true,"is_verified":true}
    {"iso8601_timestamp":"2017-07-26T06:52:36","applicant_id":"4","applicant_employer":"Uber","applicant_nationality":"Turkish","is_eligble":false}

    • logs/etl.log
    [2017-07-2713:51:37,446] - INFO - From Thread-1 - Hour 2017-07-26-05 ETL start.
    [2017-07-2713:51:37,489] - INFO - From Thread-2 - Hour 2017-07-26-06 ETL start.
    [2017-07-27 13:51:47,218] - INFO - From Thread-1 - Hour 2017-07-26-05 ETL complete,
    elapsed time: 1s.
    [2017-07-27 13:51:48,491] - INFO - From Thread-2 - Hour 2017-07-26-06 ETL complete,
    elapsed time: 1s.
