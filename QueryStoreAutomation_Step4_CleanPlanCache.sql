/**************************************************************************************************
	Step 4:  Clean Plan Cache
 *************************************************************************************************/
CREATE OR ALTER PROCEDURE QSAutomation.QueryStore_ClearPlansFromCache
AS
BEGIN
	DECLARE @QueryID bigint
		, @PlanHandle varchar(max)
		, @DynamicSQLText nvarchar(max)
 
	DECLARE PlansToClear CURSOR FAST_FORWARD FOR
	SELECT QueryID
		, CONVERT(varchar(max), CONVERT(varbinary(max), plan_handle), 1) AS PlanHandle
		FROM QSAutomation.Query 
	INNER JOIN sys.dm_exec_query_stats ON Query.QueryHash = dm_exec_query_stats.query_hash
	WHERE StatusID / 10 = 1
		OR StatusID IN (20,30)

	OPEN PlansToClear  
  
	FETCH NEXT FROM PlansToClear INTO @QueryID, @PlanHandle 

	WHILE @@FETCH_STATUS = 0  
	BEGIN  
		SELECT @DynamicSQLText = 'DBCC FREEPROCCACHE (' +  @PlanHandle + ')' 
		EXEC sp_executeSQL @DynamicSQLText

		FETCH NEXT FROM PlansToClear INTO @QueryID, @PlanHandle  
	END   

	CLOSE PlansToClear
	DEALLOCATE PlansToClear
END
