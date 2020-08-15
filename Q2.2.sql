-------------------------------------------------------------------------------
-- Delete a table if it exists
-- (just for convenience)
IF OBJECT_ID('tempdb.dbo.#activity2', 'U') IS NOT NULL 
	DROP TABLE #activity2 ;

-- Create a table without day-user duplicates
SELECT DISTINCT convert(date, date) AS dat, user_id
	INTO #activity2
	FROM activity ;


-------------------------------------------------------------------------------
-- Make a table with the lag(date) - for each date make an entry when the user was active last time
SELECT a.user_id, a.dat,
		lag(dat) OVER(PARTITION BY user_id ORDER BY dat) as prev_date
	FROM #activity2 a
	ORDER BY user_id, dat ;

-- For convenience
IF OBJECT_ID('dbo.GAP30p', 'U') IS NOT NULL 
	DROP TABLE dbo.GAP30p ;

-- Create a table "GAP30p" whith users that were last active more that 30 days ago compared to the date we observe them again
SELECT b.user_id, b.dat, b.prev_date, datediff(d, b.prev_date, b.dat) as gap
	INTO GAP30p
	FROM (
		SELECT user_id, dat,
				lag(dat) OVER(PARTITION BY user_id ORDER BY dat) as prev_date
			FROM #activity2
	) b
	WHERE datediff(d, b.prev_date, b.dat) > 30
	ORDER BY user_id, dat ;

-- See the result
--select * from GAP30p order by user_id, dat ;


-------------------------------------------------------------------------------
-- CROSS-JOINT Table with all users on all dates
IF OBJECT_ID('tempdb.dbo.#ct', 'U') IS NOT NULL 
	DROP TABLE #ct ;

SELECT *
	INTO #ct
	FROM (
		SELECT DISTINCT dat as dates
			FROM #activity2
	) a
	CROSS JOIN (
		SELECT DISTINCT user_id
		FROM #activity2
	) b ;


-------------------------------------------------------------------------------
-- For convenience
IF OBJECT_ID('dbo.Churned', 'U') IS NOT NULL 
	DROP TABLE dbo.Churned ; 

-- Create a table ("Churned") that shows for each possible date all the users that were churned at that date
SELECT #ct.dates, #ct.user_id, g.dat AS appeared_again, g.prev_date as last_appear, g.gap as absence_period
	INTO Churned
	FROM #ct
	RIGHT JOIN GAP30p g
	ON #ct.user_id = g.user_id
	WHERE datediff(d, g.dat, #ct.dates) < 0 and datediff(d, g.prev_date, #ct.dates) > 30
	ORDER BY #ct.user_id, #ct.dates, g.dat ;

-- See the result
--select * from Churned ORDER BY dates, user_id ;



-------------------------------------------------------------------------------
-- For convenience
IF OBJECT_ID('dbo.New', 'U') IS NOT NULL 
	DROP TABLE dbo.New ;

-- Create a table ("New") that shows for each possible date all the users that were first time active less than 30d ago
SELECT #ct.dates, #ct.user_id, f.first_active, datediff(d, f.first_active, #ct.dates) AS activity_length
	INTO New
	FROM #ct
	RIGHT JOIN (
		SELECT DISTINCT user_id,
				min(dat) OVER(PARTITION BY user_id ORDER BY dat) as first_active
			FROM #activity2
	) f
	ON #ct.user_id = f.user_id
	WHERE datediff(d, f.first_active, #ct.dates) < 30 AND datediff(d, f.first_active, #ct.dates) >= 0
	ORDER BY #ct.user_id, #ct.dates ;

-- See the result
--select * from New ORDER BY dates, user_id ;



-------------------------------------------------------------------------------
-- first time active more than 30d ago
IF OBJECT_ID('dbo.LongActive', 'U') IS NOT NULL 
	DROP TABLE dbo.LongActive ;

SELECT #ct.dates, #ct.user_id, f.first_active,
		datediff(d, f.first_active, #ct.dates) AS activity_length
	INTO LongActive
	FROM #ct
	RIGHT JOIN (
		SELECT DISTINCT user_id,
				min(dat) OVER(PARTITION BY user_id ORDER BY dat) as first_active
			FROM #activity2
	) f
	ON #ct.user_id = f.user_id
	WHERE datediff(d, f.first_active, #ct.dates) >= 30
	ORDER BY #ct.user_id, #ct.dates ;

-- See the result
--SELECT * FROM LongActive ORDER BY user_id, dates ;


-- just a table with lagged dates
IF OBJECT_ID('tempdb.dbo.#lagdates', 'U') IS NOT NULL 
	DROP TABLE #lagdates ;

SELECT #ct.dates,
		CONVERT(date, CONVERT(datetime, #ct.dates) - 30) AS pDate,
		#ct.user_id
	INTO #lagdates
	FROM #ct
	WHERE CONVERT(date, CONVERT(datetime, #ct.dates) - 30) >= '2000-01-01'
	ORDER BY dates, pDate, user_id ;

-- See the result
--SELECT * FROM #lagdates ORDER BY dates, user_id ;



-------------------------------------------------------------------------------
-- from a 30d ago point of view, these users were absent for 30 days
IF OBJECT_ID('dbo.GAP30to60', 'U') IS NOT NULL 
	DROP TABLE dbo.GAP30to60 ;

SELECT #lagdates.dates, #lagdates.pDate, #lagdates.user_id,
		g.dat AS appeared_again, g.prev_date as last_appear, g.gap as absence_period
	INTO GAP30to60
	FROM #lagdates
	RIGHT JOIN GAP30p g
	ON #lagdates.user_id = g.user_id
	WHERE datediff(d, g.dat, #lagdates.pDate) < 0 and datediff(d, g.prev_date, #lagdates.pDate) > 30
	ORDER BY #lagdates.user_id, #lagdates.dates, g.dat ;

-- Show the result
--SELECT * FROM GAP30to60 ORDER BY user_id, dates, pDate, appeared_again ;



-------------------------------------------------------------------------------
-- Reactivated users are those who are not in Churned but are in GAP30to60
IF OBJECT_ID('dbo.Reactivated', 'U') IS NOT NULL 
	DROP TABLE dbo.Reactivated ;

SELECT la_no_c.*, g.pDate, g.appeared_again, g.last_appear, g.absence_period
	INTO Reactivated
	FROM (
		SELECT la.*
			FROM LongActive la
			LEFT JOIN Churned c
				ON la.dates = c.dates and la.user_id = c.user_id
				WHERE c.user_id IS NULL AND c.dates IS NULL
	) la_no_c
	INNER JOIN GAP30to60 g
		ON la_no_c.dates = g.dates AND la_no_c.user_id = g.user_id
	ORDER BY la_no_c.dates, la_no_c.user_id ;

-- Show the result
--select * from Reactivated order by dates, user_id ;



-------------------------------------------------------------------------------
-- Active users are those who are not in Churned AND not Reactivated
IF OBJECT_ID('dbo.Active', 'U') IS NOT NULL 
	DROP TABLE dbo.Active ;

SELECT la_no_c.*
	INTO Active
	FROM (
		SELECT la.*
			FROM LongActive la
			LEFT JOIN Churned c
				ON la.dates = c.dates and la.user_id = c.user_id
				WHERE c.user_id IS NULL AND c.dates IS NULL
	) la_no_c
	LEFT JOIN Reactivated r
		ON la_no_c.dates = r.dates AND la_no_c.user_id = r.user_id
		WHERE r.user_id IS NULL AND r.dates IS NULL 
	ORDER BY la_no_c.dates, la_no_c.user_id ;

-- Show the result
--select * from Active order by dates, user_id ;



-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- FINAL TABLE WITH:
-- * date, #new_users, #active_users, #churn_users, #reactivated_users
SELECT #ct.dates,
		count(n.user_id) as '#new_users',
		count(a.user_id) as '#active_users',
		count(c.user_id) as '#churn_users',
		count(r.user_id) as '#reactivated_users'
	FROM #ct
		FULL OUTER JOIN New n
			ON #ct.dates = n.dates AND #ct.user_id = n.user_id
		FULL OUTER JOIN Active a
			ON #ct.dates = a.dates AND #ct.user_id = a.user_id
		FULL OUTER JOIN Churned c
			ON #ct.dates = c.dates AND #ct.user_id = c.user_id
		FULL OUTER JOIN Reactivated r
			ON #ct.dates = r.dates AND #ct.user_id = r.user_id
	GROUP BY #ct.dates
	ORDER BY #ct.dates

