/**************************************************************************************************
	Step 0:  Set Up Tables
 *************************************************************************************************/
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'QSAutomation')
BEGIN
	EXEC sp_ExecuteSQL N'CREATE SCHEMA QSAutomation'
END
GO

--Configurable settings you can customize
DROP TABLE IF EXISTS QSAutomation.Configuration

--A log of actions QSAutomation has taken
DROP TABLE IF EXISTS QSAutomation.ActivityLog

--The list of queries currently being maintained by QSAutomation
DROP TABLE IF EXISTS QSAutomation.Query
GO

--The list of StatusID to description mappings
DROP TABLE IF EXISTS QSAutomation.[Status]
GO

CREATE TABLE QSAutomation.[Status] (
	StatusID tinyint
	, StatusDescription VARCHAR(500)
)
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

CREATE TABLE QSAutomation.ActivityLog (
	ActivityLogID bigint NOT NULL IDENTITY(1,1) CONSTRAINT PK_ActivityLog PRIMARY KEY
	, ActivityDate datetime2(2) 
	, QueryID bigint NOT NULL	--No Foreign Key.  We might delete the records from the Query table, but need to keep it here for the log
	, QueryPlanID bigint		--No Foreign Key.  We might delete the records from the Query table, but need to keep it here for the log
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

--Set default configuration values
INSERT INTO QSAutomation.Configuration
VALUES (1, 'Query Unlock Start Time', NULL)
	 , (2, 'Last Query Store Reset', NULL)
	 , (3, 'Query Store Reset Count', '0')
	 , (4, 't-Statistic Threshold', '100')
	 , (5, 'DF Threshold', '10')
	 , (6, 'High Variation Duration Threshold (MS)', '500')
	 , (7, 'Mono Plan Performance Threshold (ms)', '2000')
	 , (8, 'Notification Email Address', 'chad.crawford@henryschein.com')
	 , (9, 'Email Log Level', 'Error') --Error, Warn, Info, Debug
GO

--List Status Descriptions.  This isn't used in code, but is a convenient reference for those using the tool
INSERT INTO QSAutomation.[Status] 
VALUES (0,  'Never Unlocked.  Queries with this StatusID always keep the pinned plan as long as it remains a valid plan.  These are queries we have hand-selected a plan for, or are volatile and cause issues.  We do not want any accidental changes to happen to these plans.')
	 , (1,  'New query.  This query has been pinned for less than 1 day')
	 , (2,  'Stage 1.  This query has been pinned for less than 1 week + 1 day')
	 , (3,  'Stage 2.  This query has been pinned for less than 3 weeks + 1 day')
	 , (4,  'Stage 3.  This query has been pinned for less than 5 weeks + 1 day')
	 , (11, 'New query unlocked temporarily to see if we can find a better plan')
	 , (12, 'Stage 1 query unlocked temporarily to see if we can find a better plan')
	 , (13, 'Stage 2 query unlocked temporarily to see if we can find a better plan')
	 , (14, 'Stage 3 query unlocked temporarily to see if we can find a better plan')
	 , (20, 'Mono-plan temporarily unlocked.  A mono-plan query is a long-running query that does not have a high variation in plan performance.  In other words, there isn''t a "really good" nor a "really bad" plan, the query just always seems to run long.  Queries with a StatusID of 20 regularly have any cached plans flushed to encourage new plans to surface.  As soon as a better plan surfaces, the High Variation Check grabs the better plan and pins it.')
	 , (30, 'Always unlocked.  Queries in this state are undergoing long-term searches for better plans and regularly have any cached plans flushed in order to encourage new plans to surface.')
	 , (40, 'Stable plans.  A stable plan is one that has either gone through the 5 week unlock cycle and we have the best plan available, or a mono-plan query that was unlocked and flushed, but no better plan was found.  These queries are no longer unlocked, but they are still considered during the High Variation Check and updated if new, better plans are found.')

GO