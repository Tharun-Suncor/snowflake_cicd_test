CREATE OR REPLACE PROCEDURE SANDBOX.EDWSTAGE.SP_STAGING_DIMWELLACTIVITYDAILY(
DebugFlag BOOLEAN DEFAULT FALSE,
ProcessStartDateTime TIMESTAMP_NTZ DEFAULT NULL,
ProcessEndDateTime TIMESTAMP_NTZ DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
DECLARE
  StagingDatabaseName VARCHAR;
  StagingSchemaName VARCHAR;
  StagingTableName VARCHAR;
  CteSql VARCHAR;
  Transformation_query VARCHAR;
  Type1ColString VARCHAR;
  Type1Colsortedlist VARCHAR;
  Type2Colsortedlist VARCHAR;
  BusinesskeycolString VARCHAR;
  Businesskeysortedlist VARCHAR;
  InsertCols VARCHAR;
  InsertSql VARCHAR;
  InsertCount INT := 0;
  Res RESULTSET;
BEGIN

/* TO BE DEFINED/DECLARED BY THE DEVELOPER : START */ 

StagingDatabaseName := 'SANDBOX'; 
StagingSchemaName := 'EDWSTAGE';
StagingTableName := 'DIMWELLACTIVITYDAILY';
BusinesskeycolString := 'WellActivityDailyGUID';
--Type1 columns
Type1ColString := 'WellActivityDailyGUID,WellActivityGUID,WellGUID,WellBoreGUID,WellActivityPhaseGUID,BoreholeConditions,AreaConditions,RoadConditions,Temperature,WaveConditions,Weather,Wind,ContactInformation,TVDProjectionMethod,EndDateTimeTZ,StartDateTimeTZ,NextReportingPeriod,Remarks,ReportingDay,ReportNumber,RigName,OperationsAtReportTime,FinalStatus,OperationalSummary,TimeLogSummaryCode1,TimeLogSummaryCode2,CoringSummary,WellSiteSupervisor,PrimaryContractor';

/* TO BE DEFINED/DECLARED BY THE DEVELOPER : END */ 

--Creating a set of database,schema &databasenames which used by the functions and in debug mode.
LET FnStagingDatabaseName VARCHAR := :StagingDatabaseName;
LET FnStagingSchemaName VARCHAR := :StagingSchemaName;
LET FnStagingTableName VARCHAR := :StagingTableName;

-- Call function to set session log level .
EXECUTE IMMEDIATE 'SELECT SANDBOX.EDWSTAGE.FN_ALTER_SESSION_LOG_LEVEL()';

--Get Sorted TYPE1COLUMNS list.
SELECT SANDBOX.EDWSTAGE.FN_SORT_COLUMNS(:Type1ColString) INTO :Type1Colsortedlist;

--Get Sorted TYPE2COLUMN list.
Type2Colsortedlist := (CALL SANDBOX.EDWSTAGE.SP_GENERATE_SORTED_TYPE2COLS(:FnStagingDatabaseName,:FnStagingSchemaName,:FnStagingTableName,:Type1ColString));

-- Get the Businesskey columns list.
SELECT SANDBOX.EDWSTAGE.FN_SORT_COLUMNS(:BusinesskeycolString) INTO :Businesskeysortedlist;

--Get the insert columns List.
InsertCols := (CALL SANDBOX.EDWSTAGE.SP_GET_STAGE_INSERT_COLUMNS(:FnStagingDatabaseName,:FnStagingSchemaName,:FnStagingTableName));


/* TO BE DEFINED/DECLARED BY THE DEVELOPER : START */ 
--Transformation Query
    Transformation_query := 'SELECT
        CAST (ID AS string) WellActivityDailyGUID
        ,CAST (IDJOB AS string) WellActivityGUID
        ,CAST (IDWELL AS string) WellGUID
        ,CAST (IDWELLBORE AS string) WellBoreGUID
        ,CAST (IDJOBPHASE AS string) WellActivityPhaseGUID
        ,CAST (HOLECONDITION AS string) BoreholeConditions
        ,CAST (LEASECONDITION AS string) AreaConditions
        ,CAST (ROADCONDITION AS string) RoadConditions
        ,CAST (TEMPERATURE AS decimal(38,15)) Temperature
        ,CAST (WAVECONDITIONS AS string) WaveConditions
        ,CAST (WEATHER AS string) Weather
        ,CAST (WIND AS string) Wind
        ,CAST (DAILYCONTACTS AS string) ContactInformation
        ,CAST (PROJECTEDTVDCALCMETHOD AS string) TVDProjectionMethod
        ,CAST (REPORTENDDATE AS timestamp) EndDateTimeTZ
        ,CAST (REPORTSTARTDATE AS timestamp) StartDateTimeTZ
        ,CAST (OPERATIONSNEXTREPORTPERIOD AS string) NextReportingPeriod
        ,CAST (REMARKS AS string) Remarks
        ,CAST (JOBDAY AS int) ReportingDay
        ,CAST (REPORTNUMBER AS int) ReportNumber
        ,CAST (RIGNAMES AS string) RigName
        ,CAST (OPERATIONSATREPORTTIME AS string) OperationsAtReportTime
        ,CAST (STATUSATREPORTINGTIME AS string) FinalStatus
        ,CAST (OPERATIONSSUMMARY AS string) OperationalSummary
        ,CAST (TIMELOGCODE1SUMMARY AS string) TimeLogSummaryCode1
        ,CAST (TIMELOGCODE2SUMMARY AS string) TimeLogSummaryCode2
        ,CAST (USERTEXT1 AS string) CoringSummary
        ,CAST (USERTEXT2 AS string) WellSiteSupervisor
        ,CAST (USERTEXT3 AS string) PrimaryContractor
    FROM S1_DAILYOPERATION_V2';
    
/* TO BE DEFINED/DECLARED BY THE DEVELOPER : END */  

    --Check if the transformation query contains __Deleted_Flag column
    LET DeletedFlagSql VARCHAR;
    IF (CONTAINS(UPPER(:Transformation_query),'__DELETED_FLAG')) THEN
        DeletedFlagSql := '__DELETED_FLAG AS __DELETEDFLAG';
    ELSE
        -- This covers if the transformation query doesnt contains __Deletedflag.
        DeletedFlagSql := 'FALSE AS __DELETEDFLAG';
    END IF;


/* TO BE DEFINED/DECLARED BY THE DEVELOPER : START */ 
    --Define meterialised views
    LET Mv_DAILYOPERATION_V2_S1 VARCHAR; --Source1
    --Call data_read stored procedure by passing dataread type.
    Mv_DAILYOPERATION_V2_S1 := (call SANDBOX.EDWSTAGE.SP_DATA_READ_TEST1(DatabaseName=>'SANDBOX',
                                                        SchemaName=>'RAW',
                                                        MvName=>'DAILYOPERATION_V2_MV_SILVER',
                                                        DataReadType=>'type_i_full_load',
                                                        filter=>'1=1'));
                                                        
  
    --Construct the SQL query to execute dynamically.
    CteSql := 'CREATE OR REPLACE TEMPORARY TABLE 
                '||:StagingDatabaseName ||'.'|| :StagingSchemaName ||'.'|| :StagingTableName ||'_TEMP AS ( 
                WITH S1_DAILYOPERATION_V2 AS ('|| Mv_DAILYOPERATION_V2_S1||
                '), t_q AS ( ' || Transformation_query || 
                ') SELECT *,
            TO_BINARY(SHA2(LOWER(array_to_string(array_construct_compact(' || Businesskeysortedlist || '), \'|\')),256)) AS __BUSINESSKEYHASH,
            TO_BINARY(SHA2(LOWER(array_to_string(array_construct_compact(' || Type1Colsortedlist || '), \'|\')),256)) AS __TYPE1HASH,
            TO_BINARY(SHA2(LOWER(array_to_string(array_construct_compact(' || Type2Colsortedlist || '), \'|\')),256)) AS __TYPE2HASH,
            '||DeletedFlagSql|| ',
            CURRENT_TIMESTAMP() AS __CREATEDATETIME
        FROM t_q)';

/* TO BE DEFINED/DECLARED BY THE DEVELOPER : END */ 

--Execute cteSql
EXECUTE IMMEDIATE CteSql;

--In case of debug mode, developer can decide the database/schema/tablename of the cloned table.
/* TO BE DEFINED/DECLARED BY THE DEVELOPER : START */ 
IF (DebugFlag = 1) THEN
    --Replace the empty strings with corrosponding values when running in debug mode.
    StagingDatabaseName := ''; -- Database name of cloned table.
    StagingSchemaName := ''; -- Schema name of cloned table.
    StagingTableName := ''; -- Cloned table name ex: <CLONE_DIMWELL>.
    LET create_sql VARCHAR;
    create_sql := 'CREATE OR REPLACE  TABLE '||:StagingDatabaseName ||'.'|| :StagingSchemaName ||'.'|| :StagingTableName ||'
                    CLONE ' || :FnStagingDatabaseName ||'.' || :FnStagingSchemaName ||'.'|| :FnStagingTableName ;
    EXECUTE IMMEDIATE create_sql;
END IF; 

/* TO BE DEFINED/DECLARED BY THE DEVELOPER : END */ 
    
--Insert data into stage table.
InsertSql := 'INSERT OVERWRITE INTO '||StagingDatabaseName||'.' ||StagingSchemaName ||'.' ||StagingTableName ||  '( ' || InsertCols || ') (SELECT '|| InsertCols || ' FROM '||:StagingDatabaseName ||'.'|| :StagingSchemaName ||'.'|| :StagingTableName ||'_TEMP )';

EXECUTE IMMEDIATE InsertSql; -- Execute the Insert Sql.
    
-- Execute the Insert Sql and get the inserted row count.
Res := (EXECUTE IMMEDIATE InsertSql); 
LET cur CURSOR FOR Res;
OPEN cur;
FETCH cur INTO InsertCount;
CLOSE cur;

IF (DebugFlag = 0) THEN
--Log the inserted data into Event table.
      SYSTEM$LOG('INFO', 'StoredProcedure: SP_STAGING_DIMWELLACTIVITYDAILY, StageTable: '||StagingDatabaseName||'.' ||StagingSchemaName ||'.' ||StagingTableName || ', ExecutedOn: '||current_timestamp() ||', Total number of records inserted are: ' || :InsertCount);
END IF;

 -- If the code finished successfully, return the Inserted data 
RETURN ('SP Execution finished, Inserted rows : '||: InsertCount) ;

--Handle Exception and log error details to event Table.
LET err_msg TEXT;
EXCEPTION
  WHEN STATEMENT_ERROR THEN
    err_msg := 'Error type : STATEMENT_ERROR, SQLCODE : '|| sqlcode||', SQLERRM : '|| sqlerrm ||',SQLSTATE : '||sqlstate ||', Timestamp : '||CURRENT_TIMESTAMP();
    SYSTEM$LOG('ERROR', err_msg);
    RAISE;
  WHEN OTHER THEN
    err_msg := 'Error type : OTHER ERROR, SQLCODE : '|| sqlcode||', SQLERRM : '|| sqlerrm ||',SQLSTATE : '||sqlstate ||', Timestamp : '||CURRENT_TIMESTAMP();
    SYSTEM$LOG('ERROR', err_msg);
    RAISE;
END;