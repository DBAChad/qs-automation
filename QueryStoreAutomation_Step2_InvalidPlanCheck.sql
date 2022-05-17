/**************************************************************************************************
	Step 2:  Check for invalid plans
 *************************************************************************************************/
CREATE OR ALTER PROCEDURE QSAutomation.QueryStore_InvalidPlanCheck
AS
BEGIN
	DECLARE @QueryID bigint
		, @PlanID bigint
		, @DynamicSQL nvarchar(max)
		, @BodyText nvarchar(max)
		, @QueryText nvarchar(max)
		, @SubjectText nvarchar(max)
		, @FailureReason nvarchar(max)
		, @NotificationEmailAddress varchar(max)
		, @EmailLogLevel varchar(max)

	SELECT @NotificationEmailAddress = ConfigurationValue
	FROM QSAutomation.Configuration
	WHERE ConfigurationName = 'Notification Email Address'

	SELECT @EmailLogLevel = ConfigurationValue
	FROM QSAutomation.Configuration
	WHERE ConfigurationName = 'Email Log Level'

	--Step 1:  Pinned plans that are now invalid
	DECLARE InvalidPlans CURSOR FAST_FORWARD FOR
		SELECT TOP 1 query_id, plan_id, last_force_failure_reason_desc
		FROM sys.query_store_plan
		WHERE is_forced_plan = 1
			AND last_force_failure_reason != 0


	OPEN InvalidPlans  
  
	FETCH NEXT FROM InvalidPlans INTO @QueryID, @PlanID, @FailureReason

	WHILE @@FETCH_STATUS = 0  
	BEGIN  
		SELECT @DynamicSQL = 'EXEC sp_query_store_unforce_plan @query_id = @InnerQueryID, @plan_id = @InnerPlanID'
			, @QueryText = ''
			, @SubjectText = @@SERVERNAME + ' - Invalid Plan Unforced'

		EXEC sp_executeSQL @DynamicSQL
			, N'@InnerQueryID bigint, @InnerPlanID bigint'
			, @InnerQueryID = @QueryID
			, @InnerPlanID = @PlanID

		--You also have to remove the plan so it isn't selected again
	
		SELECT @DynamicSQL = 'EXEC sp_query_store_remove_plan @plan_id = @InnerPlanID'

		EXEC sp_executeSQL @DynamicSQL
			, N'@InnerPlanID bigint'
			, @InnerPlanID = @PlanID


		IF (@EmailLogLevel IN ('Info', 'Debug'))
		BEGIN
			SELECT @QueryText = query_sql_text
			FROM sys.query_store_query
			INNER JOIN sys.query_store_query_text ON query_store_query.query_text_id = query_store_query_text.query_text_id
			WHERE query_store_query.query_id = @QueryID

			SELECT @BodyText = 'An invalid plan was forced on ' + @@Servername + '.  It has been unforced and removed from the cache.' + char(10) + char(10) +
								'QueryID: ' + CONVERT(nvarchar(max), @QueryID)  + char(10) +
								'PlanID: ' + CONVERT(nvarchar(max), @PlanID)  + char(10) + 
								'Failure Reason: ' + @FailureReason + char(10) +
								'Query Text:' +  char(10) + 
								 @QueryText


			EXEC msdb.dbo.sp_send_dbmail 
			  @profile_name = 'Default Profile'
			, @recipients = @NotificationEmailAddress
			, @body = @BodyText
			, @subject = @SubjectText

		END

		DELETE FROM QSAutomation.Query
		WHERE QueryID = @QueryID

		INSERT INTO QSAutomation.ActivityLog (QueryID, QueryPlanID, ActionDetail)
		VALUES (@QueryID, @PlanID, 'Invalid plan unforced')

		FETCH NEXT FROM InvalidPlans INTO @QueryID, @PlanID, @FailureReason
	END   
	CLOSE InvalidPlans
	DEALLOCATE InvalidPlans

	--Step 2:  Plans we thought were pinned, but are not now - some other process (manual or automated) has unpinned them.  Most often these are mono-plans that were never pinned and have been flushed from the cache.
	SELECT  Query.QueryID, Query.QueryPlanID, 'Removed record, query no longer in Query Store.  Last Status:' + CONVERT(varchar(5), Query.StatusID) AS ActionDetail
	INTO #FlushedPlans
	FROM QSAutomation.Query
	LEFT JOIN sys.query_store_plan ON Query.QueryID = query_store_plan.query_id
	WHERE  Query.StatusID <=10 --These plans are the ones we think should be pinned.  Higher than 10 are "unlocked" status and might not have anything in the cache
		AND query_store_plan.query_id IS NULL
	ORDER BY StatusID

	DELETE QSAutomation.Query
	FROM QSAutomation.Query
	INNER JOIN #FlushedPlans FlushedPlans ON Query.QueryID = FlushedPlans.QueryID

	--TODO:  This would be a good place for LogLevel for the ActivityLog and the Email in case someone wants it.  I think it's too chatty.
	INSERT INTO QSAutomation.ActivityLog (ActivityDate, QueryID, QueryPlanID, ActionDetail)
	SELECT SYSDATETIME(), QueryID, QueryPlanID, ActionDetail
	FROM #FlushedPlans
END