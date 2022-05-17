/**************************************************************************************************
	Step 8:  Clean up unused plans

	Check for any pinned plans with no executions in the past 30 days, unpin those plans.
 *************************************************************************************************/
CREATE OR ALTER PROCEDURE QSAutomation.QueryStore_CleanupUnusedPlans
AS
BEGIN
	DECLARE @DynamicSQL nvarchar(max)
		, @QueryID bigint
		, @PlanID bigint

	DECLARE OldPinndPlans CURSOR FAST_FORWARD FOR
		SELECT query_id, plan_id
		FROM sys.query_store_plan 
		WHERE is_forced_plan = 1
			AND DATEDIFF(day, last_execution_time, SYSDATETIME()) > 30

	OPEN OldPinndPlans  
  
	FETCH NEXT FROM OldPinndPlans INTO @QueryID, @PlanID 

	WHILE @@FETCH_STATUS = 0  
	BEGIN  
		SELECT @DynamicSQL = 'EXEC sp_query_store_unforce_plan @query_id = @InnerQueryID, @plan_id = @InnerPlanID'

		EXEC sp_executeSQL @DynamicSQL
			, N'@QueryID bigint, @PlanID bigint'
			, @InnerQueryID = @QueryID
			, @InnerPlanID = @PlanID

		FETCH NEXT FROM OldPinndPlans INTO @QueryID, @PlanID 
	END

	CLOSE OldPinndPlans
	DEALLOCATE OldPinndPlans
END
