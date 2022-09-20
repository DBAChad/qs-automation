/**************************************************************************************************
	Step 7:  Include Manually Pinned Plans
 *************************************************************************************************/
CREATE OR ALTER PROCEDURE QSAutomation.QueryStore_IncludeManuallyPinnedPlans
AS
BEGIN
	DECLARE @BodyText nvarchar(max)
		, @JSONResults nvarchar(max)
		, @NotificationEmailAddress varchar(max)
		, @EmailLogLevel varchar(max)

	SELECT @NotificationEmailAddress = ConfigurationValue
	FROM QSAutomation.Configuration
	WHERE ConfigurationName = 'Notification Email Address'

	SELECT @EmailLogLevel = ConfigurationValue
	FROM QSAutomation.Configuration
	WHERE ConfigurationName = 'Email Log Level'

	SELECT query_id
		, plan_id
		, query_plan_hash 
	INTO #ForcedPlans
	FROM sys.query_store_plan WHERE is_forced_plan = 1

	SELECT query_store_query.query_id
		, query_store_query.query_hash
		, ForcedPlans.plan_id
		, ForcedPlans.query_plan_hash
	INTO #ManuallyPinnedQueries
	FROM #ForcedPlans ForcedPlans
	INNER JOIN sys.query_store_query ON ForcedPlans.query_id = query_store_query.query_id
	LEFT JOIN QSAutomation.Query ON ForcedPlans.query_id = Query.QueryID
	WHERE (Query.QueryID IS NULL OR ForcedPlans.plan_id != Query.QueryPlanID)

	IF EXISTS (SELECT * FROM #ManuallyPinnedQueries)
	BEGIN

		--Reset any records
		DELETE QSAutomation.Query
		FROM QSAutomation.Query 
		INNER JOIN #ManuallyPinnedQueries ON Query.QueryID = #ManuallyPinnedQueries.query_id

		--Set the new records
		INSERT INTO QSAutomation.Query (QueryID, QueryHash, StatusID, QueryCreationDatetime, QueryPlanID, PlanHash)
		SELECT query_id, query_hash, 0, SYSDATETIME(), plan_id, query_plan_hash
		FROM #ManuallyPinnedQueries

		SELECT @BodyText = 'Manually pinned queries added to the QSAutomation tables <BR>' +
								'Server Name: ' + @@SERVERNAME + '<BR><BR>'

		INSERT INTO QSAutomation.ActivityLog (QueryID, QueryPlanID, ActionDetail)
		SELECT query_id, plan_id, @BodyText
		FROM #ManuallyPinnedQueries

		IF (@EmailLogLevel IN ('Info', 'Debug'))
		BEGIN
			SELECT @BodyText = @BodyText + '<TABLE border=1 style=''font-family:"Courier New", Courier, monospace;''>' + 
							'<TR><TH>Query ID</TH><TH>Query Hash</TH><TH>Plan ID</TH><TH>Query Plan Hash</TH></TR>' +
							CONVERT(NVARCHAR(MAX), (
								SELECT  
									(SELECT query_id AS TD FOR XML PATH(''), TYPE)
									, (SELECT CONVERT(VARCHAR(100), query_hash, 1) AS TD FOR XML PATH(''), TYPE)
									, (SELECT plan_id AS TD FOR XML PATH(''), TYPE)
									, (SELECT CONVERT(VARCHAR(100), query_plan_hash, 1) AS TD FOR XML PATH(''), TYPE)
								FROM #ManuallyPinnedQueries
								FOR XML PATH ('TR'), TYPE
							))
						+ '</TABLE>'

			EXEC msdb.dbo.sp_send_dbmail 
			@profile_name = 'Default Profile'
			, @recipients = @NotificationEmailAddress
			, @body = @Bodytext
			, @subject = 'Manually pinned queries logged' 
			, @body_format = 'HTML'
		END

	END
END

