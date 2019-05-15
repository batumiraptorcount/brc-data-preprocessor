# brc-data-preprocessor
The data preprocessor checks the raw [Batumi Raptor Count](https://www.batumiraptorcount.org) data coming straight from the [Trektellen](https://www.trektellen.org) database. It flags records containing possibly erroneous or suspicious information, but *does not delete any data*. It is up to coordinators and data technicians to decide what to do with the flagged records.

Author: Bart Hoekstra | Mail: [bart.hoekstra@batumiraptorcount.org](mailto:bart.hoekstra@batumiraptorcount.org)

## Flagged records
The following records will be flagged by the preprocessor:
- Records with invalid doublecount entries (e.g. not within 10 minutes or with the wrong distance code).
- Records containing >1 bird that is injured and/or killed (rare occurrence).
- Records lacking critical information in `datetime`, `telpost`, `speciesname`, `count` or `location` columns (very unlikely, but the possible result of a bug).
- Records of birds in >E3 (rare occurrence).
- Records containing morphs for species we do not typically record morphs of (i.e. Marsh Harriers, Booted Eagles and very rarely Montagu's Harriers).
- Records of `HB_NONJUV`, `HB_JUV`, `BK_NONJUV` and `BK_JUV` if the number of aged birds does not match with the number of counted birds (`HB` and `BK`).
- Records of Honey Buzzards that should probably be single-counted (at Station 2 during the HB focus period).
- Records of aged Honey Buzzards and Black Kites outside of expected/permitted distance codes (i.e. outside of W1-O-E1).
- Records containing unexpected combinations of sex and/or age information.
- Records of juvenile Harriers identified to species level at suspiciously long distances (W3 or E3).

## Todo
- [ ] Implement automatic download of the data, flagging of suspicious records and storing of the data in Dropbox using AWS Lambda.

## Future additions
- Implementing checks for possibly erroneous records based on some statistical rules, e.g. the (daily) phenology of a species.
