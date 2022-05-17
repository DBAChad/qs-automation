/**************************************************************************************************
	Step 6:  Fix the Broken Query Store
 *************************************************************************************************/
CREATE OR ALTER PROCEDURE QSAutomation.QueryStore_FixBrokenQueryStore
AS
BEGIN

	 DECLARE @MaxStorageSizeIncreased tinyint
		, @DynamicSQL nvarchar(max)
		, @BodyText nvarchar(max)
		, @ResetCount nvarchar(10)
		, @NotificationEmailAddress varchar(max)
		, @EmailLogLevel varchar(max)

	SELECT @NotificationEmailAddress = ConfigurationValue
	FROM QSAutomation.Configuration
	WHERE ConfigurationName = 'Notification Email Address'

	SELECT @EmailLogLevel = ConfigurationValue
	FROM QSAutomation.Configuration
	WHERE ConfigurationName = 'Email Log Level'

	--If the query store Query store is read only
	IF EXISTS (SELECT * FROM sys.database_query_store_options WHERE actual_state_desc != 'READ_WRITE')
	BEGIN
		--If the reason is 65536 (reached max storage size) or 131072 (number of statements reached the internal memory limit), increase the QS size by 2 GB
		IF EXISTS (SELECT * FROM sys.database_query_store_options WHERE actual_state_desc != 'READ_WRITE' AND readonly_reason IN (65536, 131072))
		BEGIN
			SELECT @DynamicSQL = 'ALTER DATABASE ' + DB_NAME() + ' SET QUERY_STORE = ON (MAX_STORAGE_SIZE_MB = ' + CONVERT(nvarchar(max), (max_storage_size_mb + 2048)) + ')'
				, @MaxStorageSizeIncreased = 1
			FROM sys.database_query_store_options

			EXEC sp_executesql @DynamicSQL
		END

		--Set the Query Store to read-write.
		SELECT @DynamicSQL = 'ALTER DATABASE ' + DB_NAME() + ' SET QUERY_STORE = ON (OPERATION_MODE=READ_WRITE)'
		EXEC sp_executesql @DynamicSQL

		--Set the Last Query Store Reset value to the current datetime.
		UPDATE QSAutomation.Configuration 
			SET ConfigurationValue = CONVERT(varchar(max), SYSDATETIME(), 121)
		WHERE ConfigurationName = 'Last Query Store Reset'
	
		--Increment the Query Store Reset Count.
		UPDATE QSAutomation.Configuration 
			SET ConfigurationValue = CONVERT(int, ISNULL(ConfigurationValue, '0')) + 1
		WHERE ConfigurationName = 'Query Store Reset Count'

		IF (@EmailLogLevel IN ('Info', 'Debug', 'Error', 'Warn'))
		BEGIN
			SELECT @ResetCount = ConfigurationValue 
			FROM QSAutomation.Configuration 
			WHERE ConfigurationName = 'Query Store Reset Count'

			SELECT @BodyText = 'The query store has been reset on ' + @@SERVERNAME + ' ' + @ResetCount + ' times.'  + CASE WHEN @MaxStorageSizeIncreased = 1 THEN ' Max Storage Size was increased by 2 GB.' ELSE '' END

			EXEC msdb.dbo.sp_send_dbmail 
			  @profile_name = 'Default Profile'
			, @recipients = @NotificationEmailAddress
			, @body = @BodyText
			, @subject = 'Query Store Status Reset'
		END
	END

	--If the Last Query Store Reset datetime is more than 24 hours ago, reset the Query Store Reset Count to zero.
	IF EXISTS (SELECT * FROM QSAutomation.Configuration WHERE ConfigurationName = 'Last Query Store Reset' AND DATEDIFF(hour, CONVERT(datetime2(7), ConfigurationValue), SYSDATETIME()) > 24)
	BEGIN
		UPDATE QSAutomation.Configuration 
			SET ConfigurationValue = '0'
		WHERE ConfigurationName = 'Query Store Reset Count'
	END
END	
	
	
	
