/**************************************************************************************************
	Step 3:  Check for better plans
 *************************************************************************************************/
CREATE OR ALTER PROCEDURE QSAutomation.QueryStore_BetterPlanCheck @AlwaysCheck int = 0
AS
BEGIN
	--@AlwaysCheck = 0  Only check for better plans between 9:00 and 2:00
	--@AlwaysCheck = 1  Always check for better plans (debug/testing setting)
	DECLARE @NumMinutesUnlocked int = 10
		, @QueryID bigint
		, @PlanID bigint
		, @DynamicSQL nvarchar(max)
		, @BodyText nvarchar(max)
		, @SubjectText nvarchar(max)
		, @PlanHash binary(8)
		, @NotificationEmailAddress varchar(max)
		, @EmailLogLevel varchar(max)

	SELECT @NotificationEmailAddress = ConfigurationValue
	FROM QSAutomation.Configuration
	WHERE ConfigurationName = 'Notification Email Address'

	SELECT @EmailLogLevel = ConfigurationValue
	FROM QSAutomation.Configuration
	WHERE ConfigurationName = 'Email Log Level'

	--If we already have a query under review, see if it's complete or we need to keep looking
	--Integer division allows us to check for anything between 10 and 20 without using an OR clause.
	IF EXISTS (SELECT * FROM QSAutomation.Query WHERE StatusID/10 = 1)
	BEGIN
		--If we've exceeded the unlock minutes, select the best plan.  Otherwise, do nothing.
		IF EXISTS (
			SELECT * 
			FROM QSAutomation.Configuration 
			WHERE ConfigurationName = 'Query Unlock Start Time'
				AND DATEDIFF(MINUTE, ConfigurationValue, SYSDATETIME()) >= @NumMinutesUnlocked
		)
		BEGIN
			SELECT TOP 1 
				@QueryID = Query.QueryID
				, @PlanID = query_store_runtime_stats.plan_id
				, @PlanHash = query_plan_hash
				--, SUM(count_executions * avg_duration) / SUM(count_executions) AS AverageDuration
			FROM QSAutomation.Query 
			--Left joins here.  If this is an infreqently executed or old query, we may not have a plan or stats in the query store.  In that case, we still need the QueryID
			LEFT JOIN sys.query_store_plan ON Query.QueryID = query_store_plan.query_id 
			LEFT JOIN sys.query_store_runtime_stats ON query_store_plan.plan_id = query_store_runtime_stats.plan_id
													--Require at least 2 executions (kind of an arbitrary choice, but if the plan has only been used once, we don't have as much confidence in it.
													AND count_executions > 1 
													--Regular, not Aborted or Exception
													AND execution_type = 0 
			WHERE StatusID / 10 = 1
			GROUP BY Query.QueryID, query_store_runtime_stats.plan_id, query_plan_hash
			ORDER BY SUM(count_executions * avg_duration) / SUM(count_executions)

			IF (@PlanID IS NULL) --i.e. we didn't find a better plan
			BEGIN
				SELECT @PlanID = QueryPlanID
					, @PlanHash = PlanHash
				FROM QSAutomation.Query
				WHERE QueryID = @QueryID
			END

			SELECT @DynamicSQL = 'EXEC sp_query_store_force_plan @query_id = @InnerQueryID, @plan_id = @InnerPlanID'

			EXEC sp_executeSQL @DynamicSQL
				, N'@InnerQueryID bigint, @InnerPlanID bigint'
				, @InnerQueryID = @QueryID
				, @InnerPlanID = @PlanID

			UPDATE QSAutomation.Query
			SET StatusID = CASE StatusID
								WHEN 11 THEN 2
								WHEN 12 THEN 3
								WHEN 13 THEN 4
								WHEN 14 THEN 0
								ELSE StatusID
								END
				, QueryPlanID = @PlanID
				, PlanHash = @PlanHash
				, PinDate =  SYSDATETIME()
			FROM QSAutomation.Query
			WHERE QueryID = @QueryID

			UPDATE QSAutomation.Configuration
			SET ConfigurationValue = NULL
			WHERE ConfigurationName = 'Query Unlock Start Time'

			INSERT INTO QSAutomation.ActivityLog (QueryID, QueryPlanID, ActionDetail)
			VALUES (@QueryID, @PlanID, 'Plan pinned after unpinning for a time')

			IF (@EmailLogLevel IN ('Info', 'Debug'))
			BEGIN
				SELECT @SubjectText = 'Plan pinned after testing on ' + @@Servername
					, @BodyText = 'Plan was pinned after testing for a short test to see if a better one can be found' + char(10) + char(10) +
									'Server: ' + @@Servername + char(10) +
									'QueryID: ' + CONVERT(nvarchar(max), @QueryID)  + char(10) +
									'PlanID: ' + CONVERT(nvarchar(max), @PlanID) 


				EXEC msdb.dbo.sp_send_dbmail 
					@profile_name = 'Default Profile'
				, @recipients = @NotificationEmailAddress
				, @body = @BodyText
				, @subject = @SubjectText
			END
		END
	END
	ELSE  --If we don't have a query currently unlocked for testing, check and see if it is time for one
	BEGIN
		--Only run between 9:00 AM and 2:00 PM, M-TH
		IF ((DATEPART(HOUR, SYSDATETIME()) BETWEEN 9 AND 13 AND DATEPART(WEEKDAY, SYSDATETIME()) BETWEEN 2 AND 5) OR @AlwaysCheck = 1)
		BEGIN
			SELECT TOP 1 @QueryID = QueryID
			FROM QSAutomation.Query
			WHERE  (StatusID = 1 AND DATEDIFF(DAY, QueryCreationDatetime, SYSDATETIME()) >= 1)
				OR (StatusID = 2 AND DATEDIFF(DAY, QueryCreationDatetime, SYSDATETIME()) >= 8)
				OR (StatusID = 3 AND DATEDIFF(DAY, QueryCreationDatetime, SYSDATETIME()) >= 22)
				OR (StatusID = 4 AND DATEDIFF(DAY, QueryCreationDatetime, SYSDATETIME()) >= 36)
			ORDER BY StatusID ASC, QueryCreationDatetime DESC

			--Did we find any work to do?
			IF (@QueryID IS NOT NULL)
			BEGIN
				SELECT @PlanID = plan_id
				FROM sys.query_store_plan
				WHERE query_id = @QueryID
					AND is_forced_plan = 1

				SELECT @DynamicSQL = 'EXEC sp_query_store_unforce_plan @query_id = @InnerQueryID, @plan_id = @InnerPlanID'

				EXEC sp_executeSQL @DynamicSQL
					, N'@InnerQueryID bigint, @InnerPlanID bigint'
					, @InnerQueryID = @QueryID
					, @InnerPlanID = @PlanID
		
				UPDATE QSAutomation.Configuration
				SET ConfigurationValue = SYSDATETIME()
				WHERE ConfigurationName = 'Query Unlock Start Time'

				INSERT INTO QSAutomation.ActivityLog (QueryID, QueryPlanID, ActionDetail)
				VALUES (@QueryID, @PlanID, 'Plan unpinned temporarily to check for a better plan')

				UPDATE QSAutomation.Query
				SET StatusID = StatusID + 10
				WHERE QueryID = @QueryID

				IF (@EmailLogLevel IN ('Info', 'Debug'))
				BEGIN
					SELECT @SubjectText = 'Plan unpinned for testing on ' + @@Servername
						, @BodyText = 'Plan was unpinned for a short test to see if a better one can be found' + char(10) + char(10) +
										'Server: ' + @@Servername + char(10) +
										'QueryID: ' + CONVERT(nvarchar(max), @QueryID)  + char(10) +
										'PlanID: ' + CONVERT(nvarchar(max), @PlanID) 

					EXEC msdb.dbo.sp_send_dbmail 
						@profile_name = 'Default Profile'
					, @recipients = @NotificationEmailAddress
					, @body = @BodyText
					, @subject = @SubjectText
				END
			END
		END
	END
END