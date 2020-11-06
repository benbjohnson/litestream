NOTES
=====

## RECOVERY

### REAL WAL EXISTS, SHADOW EXISTS

Scenario: Unclean close by application process.

Action: Verify last page from both match.


### REAL WAL DOESN'T EXISTS, SHADOW EXISTS

Scenario: Application closed cleanly & removed WAL.

Action: Verify last page of shadow matches database page.


### REAL WAL EXISTS, SHADOW DOESN'T EXIST

Scenario: Application wrote WAL; system crashed before shadow written/sync'd.

Action: Start new generation. 


### REAL WAL DOESN'T EXIST, SHADOW DOESN'T EXIST

Scenario: No writes have occurred since the DB was switched to WAL mode.

Action: Nothing to recover. Wait for first WAL write.

