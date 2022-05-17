/*
	TODO:  Before going to production, change all the email addresses to be ascenddbadmins@henryschein.com
	TODO:  Change all the scripts to create job steps.
	Tests are idempotent, meaning they can be run any number of times and in any order.  
	If one test fails, it may leave the DB in an inconsistent state though, so check for that.
 */
/**************************************************************************************************
	Pre-test set up data and objects
 *************************************************************************************************/
 --Set up some bad data
	DROP TABLE IF EXISTS QSAutomation.BigData
	GO
	CREATE TABLE QSAutomation.BigData (col1 char(1600), col2 INT, col3 char(1600))
	GO

	INSERT INTO QSAutomation.BigData VALUES ('abcdefghijklmn', RAND() * 100, REPLICATE('a', RAND() * 1000))
	GO
	INSERT INTO QSAutomation.BigData
	SELECT *
	FROM QSAutomation.BigData
	GO 17

	INSERT INTO QSAutomation.BigData VALUES ('opqrstuvwxyz', RAND() * 100, REPLICATE('a', RAND() * 1000))
	GO

	CREATE NONCLUSTERED INDEX IX_BigData_Col1 ON QSAutomation.BigData(Col1)
	GO

	CREATE OR ALTER PROCEDURE QSAutomation.BadPlan @Search varchar(20)
	AS
	BEGIN
		SELECT count(*) FROM QSAutomation.BigData WHERE Col1 = @Search
	END
	GO

	CREATE OR ALTER PROCEDURE QSAutomation.ResetQueryStore
	AS
	BEGIN
		DECLARE @QueryID BIGINT
			, @PlanID BIGINT

		DELETE FROM QSAutomation.Query WHERE QueryHash IN (
				0x766DA712B668299F	--High Variation query
				, 0x26E3F4EAD27D7C86 --Pulled from manually pinned plans
				, 0x3982C7BE77A0D5B7 --Long running mono-plan query
				)

		IF EXISTS (SELECT 1 
					FROM sys.query_store_query 
					INNER JOIN sys.query_store_plan ON query_store_query.query_id = query_store_plan.query_id 
					WHERE query_hash IN (
										0x766DA712B668299F	--High Variation query
										, 0x26E3F4EAD27D7C86 --Pulled from manually pinned plans
										, 0x3982C7BE77A0D5B7 --Long running mono-plan query
										)
					AND is_forced_plan = 1)
		BEGIN
			SELECT @QueryID = query_store_query.query_id
				, @PlanID = query_store_plan.plan_id
			FROM sys.query_store_query 
			INNER JOIN sys.query_store_plan ON query_store_query.query_id = query_store_plan.query_id 
			WHERE query_hash IN (
										0x766DA712B668299F	--High Variation query
										, 0x26E3F4EAD27D7C86 --Pulled from manually pinned plans
										, 0x3982C7BE77A0D5B7 --Long running mono-plan query
										)
				AND is_forced_plan = 1
			
			EXEC sp_query_store_unforce_plan @QueryID, @PlanID
		END

		IF EXISTS (SELECT 1 
					FROM sys.query_store_query 
					WHERE query_hash IN (
										0x766DA712B668299F	--High Variation query
										, 0x26E3F4EAD27D7C86 --Pulled from manually pinned plans
										, 0x3982C7BE77A0D5B7 --Long running mono-plan query
										))
		BEGIN
			SELECT @QueryID = query_store_query.query_id
			FROM sys.query_store_query 
			WHERE query_hash IN (
										0x766DA712B668299F	--High Variation query
										, 0x26E3F4EAD27D7C86 --Pulled from manually pinned plans
										, 0x3982C7BE77A0D5B7 --Long running mono-plan query
										)
			
			EXEC sp_query_store_remove_query @QueryID
		END
	END
	GO

/**************************************************************************************************
	Step 0:  Set up tables
 *************************************************************************************************/
	--Run the setup script 3 times to validate that it executes, and if the script has already been run that it still executes ok

/**************************************************************************************************
	Step 1:  High Variation Check
 *************************************************************************************************/
	--Lower the threshold for testing:
	UPDATE QSAutomation.Configuration
	SET ConfigurationValue = '10' WHERE ConfigurationName = 't-Statistic Threshold'
	
	--Reset any plan info for the query we play with
	EXEC QSAutomation.ResetQueryStore

	--Set up competing plans
	DBCC FREEPROCCACHE
	GO
	
	EXEC QSAutomation.BadPlan 'opqrstuvwxyz'
	GO 200

	DBCC FREEPROCCACHE
	GO
	
	EXEC QSAutomation.BadPlan 'abcdefghijklmn'
	GO 200

	--Run Step 1 here
	--It should identify the better plan and pin it.
	EXEC QSAutomation.QueryStore_HighVariationCheck

	select * from QSAutomation.Query
	select * from sys.query_store_plan where is_forced_plan = 1
	
	--Reset the threshold for testing:
	UPDATE QSAutomation.Configuration
	SET ConfigurationValue = '100' WHERE ConfigurationName = 't-Statistic Threshold'
	
	--Reset any plan info for the query we play with
	EXEC QSAutomation.ResetQueryStore

/**************************************************************************************************
	Step 2:  Check for invalid plans
 *************************************************************************************************/
	--Lower the threshold for testing:
	UPDATE QSAutomation.Configuration
	SET ConfigurationValue = '10' WHERE ConfigurationName = 't-Statistic Threshold'

	--Reset any plan info for the query we play with
	EXEC QSAutomation.ResetQueryStore

	--We start basically the same as the Step 1 test

	--Set up competing plans
	DBCC FREEPROCCACHE
	GO
	
	EXEC QSAutomation.BadPlan 'opqrstuvwxyz'
	GO 200

	DBCC FREEPROCCACHE
	GO
	
	EXEC QSAutomation.BadPlan 'abcdefghijklmn'

	GO 200

	--Run Step 1 here
	--It should identify the better plan and pin it.
	EXEC QSAutomation.QueryStore_HighVariationCheck

	DROP INDEX QSAutomation.BigData.IX_BigData_Col1
	GO
	
	--You may have to wait for the data flush or stats collection interval (or both).
	DBCC FREEPROCCACHE
	GO
 	EXEC QSAutomation.BadPlan 'abcdefghijklmn'

	select force_failure_count, last_force_failure_reason, last_force_failure_reason_desc, * from sys.query_store_plan where is_forced_plan = 1
	
	--Run Step 2 here 
	EXEC QSAutomation.QueryStore_InvalidPlanCheck

	select * from QSAutomation.Query				--Query is gone
	select * from sys.query_store_plan where is_forced_plan = 1	--Query is gone
	select * from QSAutomation.ActivityLog			--shows log messages

	--Reset the index
	CREATE NONCLUSTERED INDEX IX_BigData_Col1 ON QSAutomation.BigData(Col1)
	GO

	--Reset the threshold for testing:
	UPDATE QSAutomation.Configuration
	SET ConfigurationValue = '100' WHERE ConfigurationName = 't-Statistic Threshold'
	
	--Reset any plan info for the query we play with
	EXEC QSAutomation.ResetQueryStore

/**************************************************************************************************
	Step 3:  Check for better plans
 *************************************************************************************************/
	--Lower the threshold for testing:
	UPDATE QSAutomation.Configuration
	SET ConfigurationValue = '5' WHERE ConfigurationName = 't-Statistic Threshold'

	UPDATE QSAutomation.Configuration
	SET ConfigurationValue = '0' WHERE ConfigurationName = 'High Variation Duration Threshold (MS)'


	--Reset any plan info for the query we play with
	EXEC QSAutomation.ResetQueryStore

	--Replace our index with one that is slightly less efficient.
	DROP INDEX QSAutomation.BigData.IX_BigData_Col1
	GO
	CREATE NONCLUSTERED INDEX IX_BigData_Col1_Col2 ON QSAutomation.BigData(Col1, Col2)
	GO

	--Set up competing plans (just like step 1 tests, but without the index)
	DBCC FREEPROCCACHE
	GO
	
	EXEC QSAutomation.BadPlan 'opqrstuvwxyz'
	GO 200

	DBCC FREEPROCCACHE
	GO
	
	EXEC QSAutomation.BadPlan 'abcdefghijklmn'
	GO 200

	--Run Step 1 here
	EXEC QSAutomation.QueryStore_HighVariationCheck

	select * from QSAutomation.Query  --Note the plan hash, probably 0x872D21FCBEA49106
	select * from sys.query_store_plan where is_forced_plan = 1
	
	--Run Step 3 
	--Nothing should happen (because it hasn't been more than 1 day)
	EXEC QSAutomation.QueryStore_BetterPlanCheck @AlwaysCheck = 1 --Ignore the "9-2" requirement

	UPDATE QSAutomation.Query SET QueryCreationDatetime = DATEADD(DAY, -1, QueryCreationDatetime) WHERE QueryHash IN (0x766DA712B668299F)

	--Run Step 3.  It should "unpin" and start looking for a better plan
	EXEC QSAutomation.QueryStore_BetterPlanCheck @AlwaysCheck = 1 --Ignore the "9-2" requirement
	
	SELECT * FROM QSAutomation.Query			--StatusID 11
	SELECT * FROM QSAutomation.Configuration	--Query unlock start time

	--Run Step 3 again
	--Nothing should happen (it stays unlocked for 10 minutes and it hasn't been that long yet)
	EXEC QSAutomation.QueryStore_BetterPlanCheck @AlwaysCheck = 1 --Ignore the "9-2" requirement

	--Create a new, better plan with an index, drop the inferior index
	CREATE NONCLUSTERED INDEX IX_BigData_Col1 ON QSAutomation.BigData(Col1)
	DROP INDEX QSAutomation.BigData.IX_BigData_Col1_Col2
	GO

	DBCC FREEPROCCACHE
	GO
	EXEC QSAutomation.BadPlan 'opqrstuvwxyz'
	GO 200
	DBCC FREEPROCCACHE
	GO
	EXEC QSAutomation.BadPlan 'abcdefghijklmn'
	GO 200

	--Run Step 1 (it shouldn't do anything, ignoring this because it is unlocked)
	EXEC QSAutomation.QueryStore_HighVariationCheck

	UPDATE QSAutomation.Configuration
	SET ConfigurationValue = DATEADD(MINUTE, -10, CONVERT(DATETIME2(7), ConfigurationValue))
	WHERE ConfigurationName = 'Query Unlock Start Time'

	--Run Step 1 (it still shouldn't do anything, it ignores unlocked queries)
	EXEC QSAutomation.QueryStore_HighVariationCheck

	--Run Step 3 (it should pick up the better plan)
	EXEC QSAutomation.QueryStore_BetterPlanCheck @AlwaysCheck = 1 --Ignore the "9-2" requirement

	SELECT * FROM QSAutomation.Query			--Note the plan hash, probably different than before.
	SELECT * FROM QSAutomation.Configuration

	--Reset the threshold for testing:
	UPDATE QSAutomation.Configuration
	SET ConfigurationValue = '100' WHERE ConfigurationName = 't-Statistic Threshold'

	UPDATE QSAutomation.Configuration
	SET ConfigurationValue = '500' WHERE ConfigurationName = 'High Variation Duration Threshold (MS)'


	--Reset any plan info for the query we play with
	EXEC QSAutomation.ResetQueryStore

/**************************************************************************************************
	Step 4:  Clean Plan Cache
 *************************************************************************************************/

	 --Lower the threshold for testing:
	UPDATE QSAutomation.Configuration
	SET ConfigurationValue = '10' WHERE ConfigurationName = 't-Statistic Threshold'
	
	--Reset any plan info for the query we play with
	EXEC QSAutomation.ResetQueryStore
	DBCC FREEPROCCACHE
	GO

	/**********************************************************************************************
		Setup a "status 11"
	 *********************************************************************************************/
		EXEC QSAutomation.BadPlan 'opqrstuvwxyz'
		GO 200

		DBCC FREEPROCCACHE
		GO
	
		EXEC QSAutomation.BadPlan 'abcdefghijklmn'
		GO 200

		--Run Step 1 here, it should identify the better plan and pin it.
		EXEC QSAutomation.QueryStore_HighVariationCheck

		--Make it appear that it's time for this query to to be re-evaluated.
		UPDATE QSAutomation.Query SET QueryCreationDatetime = DATEADD(DAY, -1, QueryCreationDatetime) WHERE QueryHash = 0x766DA712B668299F

		--Run Step 3 
		EXEC QSAutomation.QueryStore_BetterPlanCheck @AlwaysCheck = 1 --Disabled the 9-5 check
		--This sets the Plan to StatusID 11, which will have it's plans evicted from the cache

	/**********************************************************************************************
		Setup a "status 30"
	 *********************************************************************************************/
		--Put the query into sp_executeSQL so we don't have any inadvertant whitespace or other changes (and we know the hash that comes out)
		EXEC sp_executeSQL N'SELECT TOP 10 * FROM QSAutomation.BigData WHERE col2 = 10 ORDER BY col3'
		EXEC sp_executeSQL N'SELECT TOP 10 * FROM QSAutomation.BigData WHERE col2 = 10 ORDER BY col3'
		DECLARE @query_id int
			, @plan_id INT
    
		SELECT @query_id = query_store_query.query_id
			, @plan_id = plan_id
		FROM sys.query_store_query
		INNER JOIN sys.query_store_plan ON sys.query_store_query.query_id = query_store_plan.query_id
		WHERE query_hash = 0x26E3F4EAD27D7C86

		IF @query_id IS NULL OR @plan_id IS NULL
		BEGIN
			THROW 50000, 'Whoops, didn''t find the plan', 1
		END

		EXEC sp_query_store_force_plan @query_id = @query_id, @plan_id = @plan_id
		--Run step 7 (to pull this into the QSAutomation tables)

		EXEC QSAutomation.QueryStore_IncludeManuallyPinnedPlans

		--Update the plan to be "Always unlocked"
		UPDATE QSAutomation.Query SET StatusID = 30 WHERE QueryHash = 0x26E3F4EAD27D7C86

	/**********************************************************************************************
		Setup a "status 20"
	 *********************************************************************************************/
		--Put the query into sp_executeSQL so we don't have any inadvertant whitespace or other changes (and we know the hash that comes out)
		--I had to run these two at a time (running them all together was too much)
		EXEC sp_executeSQL N'WITH BD AS (SELECT top 300 * FROM QSAutomation.BigData), BD2 AS (SELECT TOP 300 * FROM QSAutomation.BigData) SELECT * FROM BD, BD2'
		EXEC sp_executeSQL N'WITH BD AS (SELECT top 300 * FROM QSAutomation.BigData), BD2 AS (SELECT TOP 300 * FROM QSAutomation.BigData) SELECT * FROM BD, BD2'
		GO
		EXEC sp_executeSQL N'WITH BD AS (SELECT top 300 * FROM QSAutomation.BigData), BD2 AS (SELECT TOP 300 * FROM QSAutomation.BigData) SELECT * FROM BD, BD2'
		EXEC sp_executeSQL N'WITH BD AS (SELECT top 300 * FROM QSAutomation.BigData), BD2 AS (SELECT TOP 300 * FROM QSAutomation.BigData) SELECT * FROM BD, BD2'
		GO
		EXEC sp_executeSQL N'WITH BD AS (SELECT top 300 * FROM QSAutomation.BigData), BD2 AS (SELECT TOP 300 * FROM QSAutomation.BigData) SELECT * FROM BD, BD2'
		EXEC sp_executeSQL N'WITH BD AS (SELECT top 300 * FROM QSAutomation.BigData), BD2 AS (SELECT TOP 300 * FROM QSAutomation.BigData) SELECT * FROM BD, BD2'
		GO
	
		--Run step 5 (to pick up the mono-plan long query)
		EXEC QSAutomation.QueryStore_PoorPerformingMonoPlanCheck
		--Query Hash:  0x3982C7BE77A0D5B7
		-- plan hash:  0x695BC94B7C750225

	--How many plans are in the cache?  There should be at least three, one for each fo the three we set up above.  
	--I did see some very strange edge cases where sometimes a plan in the cache had the Plan Hash in the Query Hash field (i.e. the same hash for both values
	--and it was the plan's hash).  I'm not sure what causes this and it wasn't always reproducable (internet suggests it's from plan guides, but we're not using one).
	--(-http://blog.sqlgrease.com/query_hash-query_plan_hash-useful/)
		SELECT Query.QueryID
			, Query.StatusID
			,  query_plan.value('declare namespace QP="http://schemas.microsoft.com/sqlserver/2004/07/showplan"; (//QP:StmtSimple/@QueryHash)[1]', 'varchar(50)')
			, *
		FROM sys.dm_exec_cached_plans
		CROSS APPLY sys.dm_exec_query_plan(dm_exec_cached_plans.plan_handle) AS qp 
		CROSS APPLY sys.dm_exec_sql_text(plan_handle) AS st
		INNER JOIN QSAutomation.Query ON query_plan.value('declare namespace QP="http://schemas.microsoft.com/sqlserver/2004/07/showplan"; (//QP:StmtSimple/@QueryHash)[1]', 'varchar(50)') = CONVERT(VARCHAR(50), Query.QueryHash, 1)
		WHERE (
				StatusID / 10 = 1
				OR StatusID IN (20, 30)
			  )

	--Clear the plans, then see that they are no longer in cache
	EXEC QSAutomation.QueryStore_ClearPlansFromCache

	SELECT Query.QueryID
		, Query.StatusID
		, query_plan.value('declare namespace QP="http://schemas.microsoft.com/sqlserver/2004/07/showplan"; (//QP:StmtSimple/@QueryHash)[1]', 'varchar(50)')
		, *
	FROM sys.dm_exec_cached_plans
	CROSS APPLY sys.dm_exec_query_plan(dm_exec_cached_plans.plan_handle) AS qp 
	CROSS APPLY sys.dm_exec_sql_text(plan_handle) AS st
	INNER JOIN QSAutomation.Query ON query_plan.value('declare namespace QP="http://schemas.microsoft.com/sqlserver/2004/07/showplan"; (//QP:StmtSimple/@QueryHash)[1]', 'varchar(50)') = CONVERT(VARCHAR(50), Query.QueryHash, 1)
	WHERE (
			StatusID / 10 = 1
			OR StatusID IN (20, 30)
			)

	--Reset the threshold for testing:
	UPDATE QSAutomation.Configuration
	SET ConfigurationValue = '100' WHERE ConfigurationName = 't-Statistic Threshold'

	--Reset any plan info for the query we play with
	EXEC QSAutomation.ResetQueryStore


/**************************************************************************************************
	Step 5:  Poor Performing Mono-plan Check
 *************************************************************************************************/
	--Reset any plan info for the query we play with
	EXEC QSAutomation.ResetQueryStore

 	--Put the query into sp_executeSQL so we don't have any inadvertant whitespace or other changes (and we know the hash that comes out)
	--I had to run these two at a time (it was a lot of data for SSMS to process).
	EXEC sp_executeSQL N'WITH BD AS (SELECT top 300 * FROM QSAutomation.BigData), BD2 AS (SELECT TOP 300 * FROM QSAutomation.BigData) SELECT * FROM BD, BD2'
	EXEC sp_executeSQL N'WITH BD AS (SELECT top 300 * FROM QSAutomation.BigData), BD2 AS (SELECT TOP 300 * FROM QSAutomation.BigData) SELECT * FROM BD, BD2'
	GO
	EXEC sp_executeSQL N'WITH BD AS (SELECT top 300 * FROM QSAutomation.BigData), BD2 AS (SELECT TOP 300 * FROM QSAutomation.BigData) SELECT * FROM BD, BD2'
	EXEC sp_executeSQL N'WITH BD AS (SELECT top 300 * FROM QSAutomation.BigData), BD2 AS (SELECT TOP 300 * FROM QSAutomation.BigData) SELECT * FROM BD, BD2'
	GO
	EXEC sp_executeSQL N'WITH BD AS (SELECT top 300 * FROM QSAutomation.BigData), BD2 AS (SELECT TOP 300 * FROM QSAutomation.BigData) SELECT * FROM BD, BD2'
	EXEC sp_executeSQL N'WITH BD AS (SELECT top 300 * FROM QSAutomation.BigData), BD2 AS (SELECT TOP 300 * FROM QSAutomation.BigData) SELECT * FROM BD, BD2'
	
	--Run step 5 (to pick up the mono-plan long query)
	EXEC QSAutomation.QueryStore_PoorPerformingMonoPlanCheck

	--Should be StatusID:20
	SELECT * 
	FROM QSAutomation.Query
	WHERE QueryHash = 0x3982C7BE77A0D5B7 

	--Make it look like it was logged 90 minutes ago
	UPDATE QSAutomation.Query
	SET QueryCreationDatetime = DATEADD(MINUTE, -90, QueryCreationDatetime)
	WHERE QueryHash = 0x3982C7BE77A0D5B7 

	--Run step 5 (to indicate failure finding a better plan)
	EXEC QSAutomation.QueryStore_PoorPerformingMonoPlanCheck

	--Should be StatusID:0
	SELECT * 
	FROM QSAutomation.Query
	WHERE QueryHash = 0x3982C7BE77A0D5B7

	--Reset any plan info for the query we play with
	EXEC QSAutomation.ResetQueryStore

/**************************************************************************************************
	Step 6:  Fix a Broken Query Store
	For obvious reasons (I hope), don't test this on production.
 *************************************************************************************************/
	DROP TABLE IF EXISTS #RememberQSMaxSize 
	CREATE TABLE #RememberQSMaxSize (MaxSize bigint)
	
	INSERT INTO #RememberQSMaxSize
	SELECT max_storage_size_mb
	FROM sys.database_query_store_options

	/**********************************************************************************************
		Check just a regular read_only issue
	 *********************************************************************************************/
		DECLARE @DynamicSQL nvarchar(max)
		SELECT @DynamicSQL = 'ALTER DATABASE ' + DB_NAME() + ' SET QUERY_STORE (OPERATION_MODE = READ_ONLY)'

		EXEC sp_executesql @DynamicSQL

		SELECT * FROM sys.database_query_store_options

		--Execute Step 6
		EXEC QSAutomation.QueryStore_FixBrokenQueryStore

		SELECT * FROM sys.database_query_store_options

	/**********************************************************************************************
		Create an out-of-space condition
	 *********************************************************************************************/
		DECLARE @DynamicSQL nvarchar(max)
		SELECT @DynamicSQL = 'ALTER DATABASE ' + DB_NAME() + ' SET QUERY_STORE = ON (MAX_STORAGE_SIZE_MB = ' + CONVERT(nvarchar(max), (IIF(current_storage_size_mb = 1, 1, current_storage_size_mb - 1))) + ')'
		FROM sys.database_query_store_options

		EXEC sp_executesql @DynamicSQL

		--Wait until it goes read-only. Sometimes this is hard to trigger deliberately.
		--May take awhile - perhaps even flush_interval_seconds (which defaults to 15 minutes).
		--Also, it seems like some kind of activity is necessary, let's create a bunch of unique queries
		DECLARE @X int = 1
			, @DynamicSQL nvarchar(max)

		WHILE @X < 500
		BEGIN
			SELECT @DynamicSQL = 'SELECT TOP 10 ' + CONVERT(varchar(10), @X) + ', * FROM QSAutomation.BigData WHERE Col1 = ''10'''
				, @X = @X + 1
			EXEC sp_executesql @DynamicSQL
			EXEC sp_executesql @DynamicSQL
		END

		SELECT max_storage_size_mb, * FROM sys.database_query_store_options

		--Execute Step 6
		EXEC QSAutomation.QueryStore_FixBrokenQueryStore
				
		SELECT max_storage_size_mb, * FROM sys.database_query_store_options

		--Reset the max size
		DECLARE @DynamicSQL nvarchar(max)
		SELECT @DynamicSQL = 'ALTER DATABASE ' + DB_NAME() + ' SET QUERY_STORE = ON (MAX_STORAGE_SIZE_MB = ' + CONVERT(nvarchar(max), (MaxSize - 1)) + ')'
		FROM #RememberQSMaxSize

		EXEC sp_executesql @DynamicSQL
		
		SELECT max_storage_size_mb, * FROM sys.database_query_store_options

	/**********************************************************************************************
		Validate that the counter resets after a cooldown
	 *********************************************************************************************/
		SELECT * 
		FROM QSAutomation.Configuration 
		WHERE ConfigurationName IN ('Last Query Store Reset', 'Query Store Reset Count')

		UPDATE QSAutomation.Configuration 
		SET ConfigurationValue = DATEADD(HOUR,-26, CONVERT(datetime2(7), ConfigurationValue))
		WHERE ConfigurationName = 'Last Query Store Reset'

		--Execute Step 6
		EXEC QSAutomation.QueryStore_FixBrokenQueryStore

		SELECT * 
		FROM QSAutomation.Configuration 
		WHERE ConfigurationName IN ('Last Query Store Reset', 'Query Store Reset Count')

/**************************************************************************************************
	Step 7:  Include Manually Pinned Plans
 *************************************************************************************************/
	--Reset any plan info for the query we play with
	EXEC QSAutomation.ResetQueryStore

 	--Put the query into sp_executeSQL so we don't have any inadvertant whitespace or other changes (and we know the hash that comes out)
	EXEC sp_executeSQL N'SELECT TOP 10 * FROM QSAutomation.BigData WHERE col2 = 10 ORDER BY col3'
	EXEC sp_executeSQL N'SELECT TOP 10 * FROM QSAutomation.BigData WHERE col2 = 10 ORDER BY col3'
	DECLARE @query_id int
		, @plan_id INT
    
	SELECT @query_id = query_store_query.query_id
		, @plan_id = plan_id
	FROM sys.query_store_query
	INNER JOIN sys.query_store_plan ON sys.query_store_query.query_id = query_store_plan.query_id
	WHERE query_hash = 0x26E3F4EAD27D7C86 

	IF @query_id IS NULL OR @plan_id IS NULL
	BEGIN
		THROW 50000, 'Whoops, didn''t find the plan', 1
	END

	EXEC sp_query_store_force_plan @query_id = @query_id, @plan_id = @plan_id

	--Run step 7 (to pull this into the QSAutomation tables)
	EXEC QSAutomation.QueryStore_IncludeManuallyPinnedPlans

	--Reset any plan info for the query we play with
	EXEC QSAutomation.ResetQueryStore

/**************************************************************************************************
	Step 8:  Clean up unused plans
 *************************************************************************************************/
	--This is a difficult one to test since you can't directly manipulate the contents of the query
	--store to change the record of how long something has been in there.   The best you can do is
	--check before running to see if anything _should_ be cleaned up and then verify that it was.
	--But if nothing needs to be cleaned up, the test is inconclusive (i.e. doesn't tell you if the
	--code worked or not.

	--Is there anything to clean up?
	SELECT query_id, plan_id
	FROM sys.query_store_plan 
	WHERE is_forced_plan = 1
		AND DATEDIFF(day, last_execution_time, SYSDATETIME()) > 30

	--Does this clean anything up?
	EXEC QSAutomation.QueryStore_CleanupUnusedPlans

	--Anything left?
	SELECT query_id, plan_id
	FROM sys.query_store_plan 
	WHERE is_forced_plan = 1
		AND DATEDIFF(day, last_execution_time, SYSDATETIME()) > 30

/**************************************************************************************************
	Clean up
 *************************************************************************************************/
	DROP PROCEDURE QSAutomation.BadPlan
	DROP PROCEDURE QSAutomation.ResetQueryStore
	DROP TABLE QSAutomation.BigData
