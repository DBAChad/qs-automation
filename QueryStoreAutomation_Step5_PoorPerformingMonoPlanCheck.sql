/**************************************************************************************************
	Step 5:  Poor performing mono-plan check
	Consider combining this with Step 1, since they use the same dataset to find queries
 *************************************************************************************************/
CREATE OR ALTER PROCEDURE QSAutomation.QueryStore_PoorPerformingMonoPlanCheck
AS
BEGIN
	DECLARE @HighVariationDurationThreshold_MS int
		, @MonoPlanPerformanceThreshold int
		, @TStatisticThreshold int
		, @DFThreshold int
		, @DynamicSQL nvarchar(max)
		, @QueryID bigint
		, @FastestPlanID bigint
		, @SlowestPlanID bigint
		, @MSDelta bigint
		, @tStatistic numeric(19,5)
		, @DF bigint
		, @QueryText nvarchar(max)
		, @QueryHash binary(8)
		, @PlanHash binary(8)
		, @BodyText nvarchar(max)
		, @NotificationEmailAddress varchar(max)
		, @EmailLogLevel varchar(max)

	SELECT @HighVariationDurationThreshold_MS = ConfigurationValue
	FROM QSAutomation.Configuration
	WHERE ConfigurationName = 'High Variation Duration Threshold (MS)'

	SELECT @MonoPlanPerformanceThreshold = ConfigurationValue
	FROM QSAutomation.Configuration
	WHERE ConfigurationName = 'Mono Plan Performance Threshold (ms)'

	SELECT @NotificationEmailAddress = ConfigurationValue
	FROM QSAutomation.Configuration
	WHERE ConfigurationName = 'Notification Email Address'
	
	SELECT @EmailLogLevel = ConfigurationValue
	FROM QSAutomation.Configuration
	WHERE ConfigurationName = 'Email Log Level'

	--If the necessary parameters do not exist, error and exit the batch
	IF @HighVariationDurationThreshold_MS IS NULL OR @MonoPlanPerformanceThreshold IS NULL
	BEGIN
		RAISERROR ('Configuration Values Missing', 16, 1)
		RETURN
	END

	--We'll use duration.  I/O, CPU and Memory are also available and would be good options too.
	SELECT plan_id
		, SUM(count_executions * avg_duration) / SUM(count_executions) AS AverageDuration
		, SQRT(SUM((count_executions - 1) * SQUARE(stdev_duration)) / SUM(count_executions - 1)) AS PooledDurationSTDev
		, SUM(count_executions) AS N
	INTO #PlanStats
	FROM sys.query_store_runtime_stats
	WHERE 
		--If the count_executions is 1, the SD is zero and no information is added to the PooledSD.  So let's just exclude those from both the average and PooledSD
		count_executions > 1 
		--Regular, not Aborted or Exception
		AND execution_type = 0 
	GROUP BY plan_id
	HAVING SUM(count_executions) >= 100

	SELECT query_id
		, query_store_plan.plan_id
		, AverageDuration
		, PooledDurationSTDev
		, N
		, is_forced_plan
		, query_plan_hash
		, ROW_NUMBER() OVER (PARTITION BY query_id ORDER BY AverageDuration) AS FastestPlan
		, ROW_NUMBER() OVER (PARTITION BY query_id ORDER BY AverageDuration DESC) AS SlowestPlan
	INTO #RankedPlanStats
	FROM sys.query_store_plan
	INNER JOIN #PlanStats ON query_store_plan.plan_id = #PlanStats.plan_id

	SELECT query_id
		, MAX(CASE WHEN SlowestPlan = 1 THEN plan_id ELSE NULL END) AS SlowestPlan
		, MAX(CASE WHEN FastestPlan = 1 THEN plan_id ELSE NULL END) AS FastestPlan
		, MAX(CASE WHEN SlowestPlan = 1 THEN query_plan_hash ELSE NULL END) AS SlowestPlanHash
		, MAX(CASE WHEN FastestPlan = 1 THEN query_plan_hash ELSE NULL END) AS FastestPlanHash
		, MAX(CASE WHEN SlowestPlan = 1 THEN AverageDuration ELSE NULL END) AS SlowestPlanDuration
		, MAX(CASE WHEN FastestPlan = 1 THEN AverageDuration ELSE NULL END) AS FastestPlanDuration
		, (MAX(CASE WHEN SlowestPlan = 1 THEN AverageDuration ELSE NULL END) - MAX(CASE WHEN FastestPlan = 1 THEN AverageDuration ELSE NULL END))
			/ 
				(
				--Begin Pooled SD
				SQRT(
						(
							MAX(CASE WHEN SlowestPlan = 1 THEN SQUARE(PooledDurationSTDev) * (N-1) ELSE NULL END)
							+ MAX(CASE WHEN FastestPlan = 1 THEN SQUARE(PooledDurationSTDev) * (N-1) ELSE NULL END)
						)
						/ (1.0 * (MAX(CASE WHEN SlowestPlan = 1 THEN N ELSE NULL END)
							+ MAX(CASE WHEN FastestPlan = 1 THEN N ELSE NULL END)
							- 2))
					)
				--End Pooled SD
				* SQRT(
						(1.0 / MAX(CASE WHEN SlowestPlan = 1 THEN N ELSE NULL END))
						+ (1.0 / MAX(CASE WHEN FastestPlan = 1 THEN N ELSE NULL END))
					  )
				)
		AS tStatistic

		, (MAX(CASE WHEN SlowestPlan = 1 THEN N ELSE NULL END)) AS N
	INTO #QueryStats
	FROM #RankedPlanStats
	--This is the divergence point between Step 1 and Step 5.
	WHERE (FastestPlan = 1 AND SlowestPlan = 1)
	GROUP BY query_id
	HAVING SUM(CONVERT(int, is_forced_plan)) = 0

	--We don't validate that the [Step 1: High Variation Check] wouldn't have caught this - if it would have,
	--it will the next time it runs so that's ok.
	SELECT #QueryStats.query_id
		, query_store_query.query_hash
		, query_sql_text
		, SlowestPlanDuration
	INTO #MonoPlanQueries
	FROM #QueryStats
	INNER JOIN sys.query_store_query ON #QueryStats.query_id = query_store_query.query_id
	INNER JOIN sys.query_store_query_text ON query_store_query.query_text_id = query_store_query_text.query_text_id
	LEFT JOIN QSAutomation.Query ON #QueryStats.query_id = Query.QueryID
	WHERE SlowestPlanDuration > @MonoPlanPerformanceThreshold
		AND Query.QueryID IS NULL

	INSERT INTO QSAutomation.Query (QueryID, QueryHash, StatusID, QueryCreationDatetime)
	SELECT query_id
		, query_hash
		, 20
		, SYSDATETIME()
	FROM #MonoPlanQueries

	IF (EXISTS (SELECT * FROM #MonoPlanQueries)) AND (@EmailLogLevel IN ('Info', 'Debug'))
	BEGIN
		--We can't use the @Query parameter of sp_send_dbmail because that runs in a separate session (and doesn't have
		--access to the temp tables).  So we use XML methods to create a HTML table
		SELECT @BodyText = 'The queries below only have one plan and ran longer than the threshold: <BR>'+
							'Query Count: ' + CONVERT(VARCHAR(10), COUNT(*)) + '<BR>' +
							'Server Name: ' + @@SERVERNAME + '<BR>' +
							'Details: <BR><BR>'
		FROM #MonoPlanQueries

		SELECT @BodyText = @BodyText + '<TABLE border=1 style=''font-family:"Courier New", Courier, monospace;''>' + 
								'<TR><TH>Query ID</TH><TH>Query Hash</TH><TH>Query Text</TH><TH>Query Duration</TH></TR>' +
								CONVERT(NVARCHAR(MAX), (
									SELECT  
										(SELECT query_id AS TD FOR XML PATH(''), TYPE)
										, (SELECT CONVERT(VARCHAR(100), query_hash, 1) AS TD FOR XML PATH(''), TYPE)
										, (SELECT query_sql_text AS TD FOR XML PATH(''), TYPE)
										, (SELECT CONVERT(VARCHAR(100), CONVERT(NUMERIC(28,2), SlowestPlanDuration)) AS TD FOR XML PATH(''), TYPE)
									--SELECT query_id AS TD
									--	, query_hash AS [TD1]
									--	, query_sql_text AS [TD2]
									--	, SlowestPlanDuration AS [TD3]
									FROM #MonoPlanQueries
									FOR XML PATH ('TR'), TYPE
								))
							+ '</TABLE>'

		
		EXEC msdb.dbo.sp_send_dbmail 
		  @profile_name = 'Default Profile'
		, @recipients = @NotificationEmailAddress
		, @body = @BodyText
		, @subject = 'NOTE: Long-running mono-plan queries' 
		, @body_format = 'HTML'
		
	END
	

	SELECT * 
	INTO #UnresolvedMonoPlans
	FROM QSAutomation.Query 
	WHERE StatusID = 20
		AND DATEDIFF(MINUTE, QueryCreationDatetime, SYSDATETIME()) >= 60

	IF EXISTS (SELECT * FROM #UnresolvedMonoPlans)
	BEGIN
		UPDATE QSAutomation.Query 
		SET StatusID = 0
		FROM QSAutomation.Query 
		INNER JOIN #UnresolvedMonoPlans ON Query.QueryID = #UnresolvedMonoPlans.QueryID
	
		INSERT INTO QSAutomation.ActivityLog (QueryID, ActionDetail)
		SELECT QueryID
			, 'Attempts to find additional plans for a poorly performing mono-plan query failed'
		FROM #UnresolvedMonoPlans

		IF (@EmailLogLevel IN ('Info', 'Debug'))
		BEGIN
			SELECT @Bodytext = 'Attempts to find additional plans for poorly performing mono-plan query(ies) failed <BR>' +
								'Query Count: ' + CONVERT(VARCHAR(10), COUNT(*)) + '<BR>' +
								'Server Name: ' + @@SERVERNAME + '<BR>' +
								'Details: <BR><BR>'
			FROM #UnresolvedMonoPlans

			SELECT @BodyText = @BodyText + '<TABLE border=1 style=''font-family:"Courier New", Courier, monospace;''>' + 
									'<TR><TH>Query ID</TH><TH>Query Hash</TH><TH>Query Text</TH></TR>' +
									CONVERT(NVARCHAR(MAX), (
										SELECT  
											(SELECT QueryID AS TD FOR XML PATH(''), TYPE)
											, (SELECT CONVERT(VARCHAR(100), QueryHash, 1) AS TD FOR XML PATH(''), TYPE)
											, (SELECT query_sql_text AS TD FOR XML PATH(''), TYPE)
											FROM #UnresolvedMonoPlans
											LEFT JOIN sys.query_store_query ON #UnresolvedMonoPlans.QueryID = query_store_query.query_id
											LEFT JOIN sys.query_store_query_text ON sys.query_store_query.query_text_id = query_store_query_text.query_text_id
											FOR XML PATH ('TR'), TYPE
									))
								+ '</TABLE>'
		
			EXEC msdb.dbo.sp_send_dbmail 
			  @profile_name = 'Default Profile'
			, @recipients = @NotificationEmailAddress
			, @body = @BodyText
			, @subject = 'ALARM: Long-running mono-plan query unresolved' 
			, @body_format = 'HTML'
		END
	END
END