# brc-data-preprocessor <a href="https://www.batumiraptorcount.org"><img src="https://static1.squarespace.com/static/5b33912fb27e39bd89996b9d/t/5b33ac53352f535c7e8effcb/1560539069142/?format=120w" alt="BRC logo" align="right"></a>
The data preprocessor checks the raw [Batumi Raptor Count](https://www.batumiraptorcount.org) data coming straight from the [Trektellen](https://www.trektellen.org) database. It flags records containing possibly erroneous or suspicious information, but *does not delete any data*. It is up to coordinators and data technicians to decide what to do with the flagged records.

Author: Bart Hoekstra | Mail: [bart.hoekstra@batumiraptorcount.org](mailto:bart.hoekstra@batumiraptorcount.org)

## Flagged records
The following records will be flagged by the preprocessor:
- Records with invalid doublecount entries (e.g. not within 10 minutes or with the wrong distance code).
- Records containing >1 bird that is injured and/or killed (rare occurrence).
- Records lacking critical information in `datetime`, `telpost`, `speciesname`, `count` or `location` columns (very unlikely, but the possible result of a bug).
- Records of birds in >E3 (rare occurrence).
- Records with registered morphs for all species other than Booted Eagles.
- Records of `HB_NONJUV`, `HB_JUV`, `BK_NONJUV` and `BK_JUV` if the number of aged birds is higher than the number of counted birds (`HB` and `BK`) within a 10-minute window around the age record.
- Records of Honey Buzzards that should probably be single-counted (at Station 2 during the HB focus period).
- Records of aged Honey Buzzards and Black Kites outside of expected/permitted distance codes (i.e. outside of W1-O-E1).
- Records containing unexpected combinations of sex and/or age information.
- Records of juvenile Harriers identified to species level at suspiciously long distances (W3 or E3).
- Records with no timestamps, which are set to 00:00:00 during processing.

## Todo
- [x] Implement automatic download of the data, flagging of suspicious records and storing of the data in Dropbox using AWS Lambda.
- [x] Automatically add `START` and `END` records to fetched data based on count start and end times.

## Future additions
- [ ] Implement checks for possibly erroneous records based on some statistical rules, e.g. the expected (daily) phenology of a species.

## Build Lambda deployment package (requires Docker and AWS CLI)
1. Clone this repository.
2. Copy `fetcher.py`, `preprocessor.py` and `requirements.txt` to `lambda/` directory.
    ```
    cp $(pwd)/{fetcher.py,preprocessor.py,requirements.txt} lambda/`.
    ```
3. Build the [Docker](https://docs.docker.com/install/) image to generate a deployment package of function code. 
    ```
    docker build -t brc-data-preprocessor .
    ```
4. Run the Docker container to generate a `function.zip` deployment package in `lambda/`. 
    ```
    docker run -it -v $(pwd)/lambda:/lambda brc-data-preprocessor bash /lambda/deploypackage.sh
    ```
5. Update Lambda function with new package `function.zip` through the [AWS CLI](https://aws.amazon.com/cli/). 
    ```
    aws lambda update-function-code --function-name brc-data-preprocessor --zip-file fileb://lambda/function.zip
    ```
