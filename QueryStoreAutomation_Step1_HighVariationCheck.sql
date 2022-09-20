/**************************************************************************************************
	Step 1:  High Variation Check
 *************************************************************************************************/
CREATE OR ALTER PROCEDURE QSAutomation.QueryStore_HighVariationCheck
AS
BEGIN
	DECLARE @HighVariationDurationThreshold_MS int
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

	SELECT @TStatisticThreshold = ConfigurationValue
	FROM QSAutomation.Configuration
	WHERE ConfigurationName = 't-Statistic Threshold'

	SELECT @DFThreshold = ConfigurationValue
	FROM QSAutomation.Configuration
	WHERE ConfigurationName = 'DF Threshold'

	SELECT @NotificationEmailAddress = ConfigurationValue
	FROM QSAutomation.Configuration
	WHERE ConfigurationName = 'Notification Email Address'

	SELECT @EmailLogLevel = ConfigurationValue
	FROM QSAutomation.Configuration
	WHERE ConfigurationName = 'Email Log Level'

	--If the necessary parameters do not exist, error and exit the batch
	IF @HighVariationDurationThreshold_MS IS NULL OR @TStatisticThreshold IS NULL OR @DFThreshold IS NULL
	BEGIN
		RAISERROR ('Configuration Values Missing: High Variation Duration Threshold (MS), t-Statistic Threshold, or DF Threshold.', 16, 1)
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
	--Rather than limiting here, we'll use the DF


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

		, (MAX(CASE WHEN SlowestPlan = 1 THEN N ELSE NULL END)
			+ MAX(CASE WHEN FastestPlan = 1 THEN N ELSE NULL END)
			- 2) 	AS DF
	INTO #QueryStats
	FROM #RankedPlanStats
	WHERE (FastestPlan = 1 AND SlowestPlan != 1)
		OR (FastestPlan != 1 AND SlowestPlan = 1)
	GROUP BY query_id
	HAVING SUM(CONVERT(int, is_forced_plan)) = 0



	DECLARE HighVariationPlans CURSOR FAST_FORWARD FOR
		SELECT #QueryStats.query_id
			, #QueryStats.SlowestPlan
			, #QueryStats.FastestPlan
			, SlowestPlanDuration - FastestPlanDuration AS MSDelta
			, tStatistic
			, DF
			, query_store_query.query_hash
			, query_store_query_text.query_sql_text
			, #QueryStats.FastestPlanHash
		FROM #QueryStats
		INNER JOIN sys.query_store_query ON #QueryStats.query_id = query_store_query.query_id
		INNER JOIN sys.query_store_query_text ON query_store_query.query_text_id = query_store_query_text.query_text_id
		LEFT JOIN QSAutomation.Query ON #QueryStats.query_id = Query.QueryID
		WHERE tStatistic > @TStatisticThreshold
			AND (SlowestPlanDuration - FastestPlanDuration) > @HighVariationDurationThreshold_MS
			AND DF > @DFThreshold
			AND (Query.QueryID IS NULL OR (Query.StatusID NOT IN (0, 30) AND ISNULL(Query.QueryPlanID, -1) <> ISNULL(#QueryStats.FastestPlan, -1)))
		ORDER BY SlowestPlanDuration - FastestPlanDuration DESC

	OPEN HighVariationPlans  
  
	FETCH NEXT FROM HighVariationPlans INTO @QueryID, @SlowestPlanID, @FastestPlanID, @MSDelta, @tStatistic, @DF, @QueryHash, @QueryText, @PlanHash

	WHILE @@FETCH_STATUS = 0  
	BEGIN  
		SELECT @DynamicSQL = 'EXEC sp_query_store_force_plan @query_id = @InnerQueryID, @plan_id = @InnerPlanID'

		EXEC sp_executeSQL @DynamicSQL
			, N'@InnerQueryID bigint, @InnerPlanID bigint'
			, @InnerQueryID = @QueryID
			, @InnerPlanID = @FastestPlanID

		--Clean up any existing record
		DELETE FROM QSAutomation.Query WHERE QueryID = @QueryID

		INSERT INTO QSAutomation.Query (QueryID, QueryHash, StatusID, QueryCreationDatetime, QueryPlanID, PlanHash, PinDate)
		VALUES (@QueryID, @QueryHash, 1, SYSDATETIME(), @FastestPlanID, @PlanHash, SYSDATETIME())

		--INSERT INTO QSAutomation.QueryPlan (QueryPlanID, QueryID, PlanHash, PinDate)
		--VALUES (@FastestPlanID, @QueryID, @PlanHash, SYSDATETIME())

		SELECT @BodyText = 'New Plan Pinned: ' + char(10) +
							'Server Name: ' + @@SERVERNAME + char(10) +
							'QueryID: ' + CONVERT(nvarchar(max), @QueryID)  + char(10) +
							'PlanID: ' + CONVERT(nvarchar(max), @FastestPlanID)  + char(10) + 
							'Slow Plan: ' + CONVERT(nvarchar(max), @SlowestPlanID)  + char(10) + 
							'Performance Delta: ' + CONVERT(nvarchar(max), @MSDelta)  + char(10) + 
							't-Statistic: ' + CONVERT(nvarchar(max), @tStatistic)  + char(10) + 
							'DF: ' + CONVERT(nvarchar(max), @DF)  + char(10) + 
							'Query Text:' +  char(10) + 
							 @QueryText

		INSERT INTO QSAutomation.ActivityLog (QueryID, QueryPlanID, ActionDetail)
		VALUES (@QueryID, @FastestPlanID, @BodyText)

		IF (@EmailLogLevel IN ('Info', 'Debug'))
		BEGIN
			EXEC msdb.dbo.sp_send_dbmail 
			  @profile_name = 'Default Profile'
			, @recipients = @NotificationEmailAddress
			, @body = @BodyText
			, @subject = 'Query Store Plan Pinned'
		END

		FETCH NEXT FROM HighVariationPlans INTO @QueryID, @SlowestPlanID, @FastestPlanID, @MSDelta, @tStatistic, @DF, @QueryHash, @QueryText, @PlanHash
	END

	CLOSE HighVariationPlans
	DEALLOCATE HighVariationPlans
END