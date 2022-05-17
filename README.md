# QS-Automation

QS-Automation is a collection of SQL Scripts automating tasks related to Microsoft's Query Store.

## Installation

Run the scripts in numerical order to create the tables and stored procedures


## Usage

```
--Identify plans with significant performance differences and pin the faster plan
EXEC QSAutomation.QueryStore_HighVariationCheck

--Check for plans that are no longer valid (e.g. schema has changed) and remove them
EXEC QSAutomation.QueryStore_InvalidPlanCheck

--If a plan has been pinned for awhile, unpin it to see if we can find an even better one now, then repin it later
EXEC QSAutomation.QueryStore_BetterPlanCheck --Ignored unless it is M-Th 9:00 AM - 2:00 PM (we need a 'daytime' load)
EXEC QSAutomation.QueryStore_BetterPlanCheck @AlwaysCheck = 1 --Ignore the "9-2" requirement

--Clear plans for queries that are under investigation
EXEC QSAutomation.QueryStore_ClearPlansFromCache

--Check for queries that are long-running, but only have one plan
EXEC QSAutomation.QueryStore_PoorPerformingMonoPlanCheck

--Check Query Store health and fix any issues
EXEC QSAutomation.QueryStore_FixBrokenQueryStore

--If a query has been manually pinned (outside of QSAutomation), enroll the query for future maintenance
EXEC QSAutomation.QueryStore_IncludeManuallyPinnedPlans

--Clean up any plans that have not been used recently to free up space
EXEC QSAutomation.QueryStore_CleanupUnusedPlans
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)

MIT License

Copyright (c) 2021 Chad Crawford

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
