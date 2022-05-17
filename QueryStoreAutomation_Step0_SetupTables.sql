/**************************************************************************************************
	Step 0:  Set Up Tables
 *************************************************************************************************/
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'QSAutomation')
BEGIN
	EXEC sp_ExecuteSQL N'CREATE SCHEMA QSAutomation'
END
GO

DROP TABLE IF EXISTS QSAutomation.Configuration
DROP TABLE IF EXISTS QSAutomation.ActivityLog
DROP TABLE IF EXISTS QSAutomation.QueryPlan
DROP TABLE IF EXISTS QSAutomation.Query
GO

CREATE TABLE QSAutomation.Query (
	QueryID bigint NOT NULL CONSTRAINT PK_Query PRIMARY KEY
	, QueryHash binary(8)
	, StatusID tinyint
	, QueryCreationDatetime datetime2(2)

	, QueryPlanID bigint NULL
	, PlanHash binary(8) NULL
	, PinDate datetime2(2) NULL
)
GO

/*
CREATE TABLE QSAutomation.QueryPlan (
	QueryPlanID bigint NOT NULL CONSTRAINT PK_QueryPlan PRIMARY KEY
	, QueryID bigint NOT NULL CONSTRAINT FK_QueryPlan_Query REFERENCES QSAutomation.Query(QueryID)
	, PlanHash binary(8)
	, PinDate datetime2(2)
)
GO
*/

CREATE TABLE QSAutomation.ActivityLog (
	ActivityLogID bigint NOT NULL IDENTITY(1,1) CONSTRAINT PK_ActivityLog PRIMARY KEY
	, ActivityDate datetime2(2) 
	, QueryID bigint NOT NULL	--No FK, we might delete the records from the Query table, but need to keep it here for the log
	, QueryPlanID bigint		--No FK, we might delete the records from the Query table, but need to keep it here for the log
	, ActionDetail nvarchar(max)
)
GO

ALTER TABLE QSAutomation.ActivityLog ADD CONSTRAINT DF_ActivityLog_ActivityDate DEFAULT SYSDATETIME() FOR ActivityDate
GO

CREATE TABLE QSAutomation.Configuration (
	ConfigurationID int
	, ConfigurationName varchar(100)
	, ConfigurationValue varchar(100)
)
GO

--Set default values
INSERT INTO QSAutomation.Configuration
VALUES (1, 'Query Unlock Start Time', NULL)
	 , (2, 'Last Query Store Reset', NULL)
	 , (3, 'Query Store Reset Count', '0')
	 , (4, 't-Statistic Threshold', '100')
	 , (5, 'DF Threshold', '10')
	 , (6, 'High Variation Duration Threshold (MS)', '500')
	 , (7, 'Mono Plan Performance Threshold (ms)', '30000')
	 , (8, 'Notification Email Address', 'chad.crawford@henryschein.com')
	 , (9, 'Email Log Level', 'Error') --Error, Warn, Info, Debug
GO
