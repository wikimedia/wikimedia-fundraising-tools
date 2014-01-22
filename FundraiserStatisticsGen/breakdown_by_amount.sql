-- mysql -BN civicrm < pie.sql

set @start = "20120701";
set @end = "20130630";

SELECT 10, SUM(total_amount), AVG(total_amount), COUNT(total_amount)
    FROM civicrm_contribution
    WHERE (receive_date BETWEEN @start AND @end) AND total_amount BETWEEN 0 AND 10;
SELECT 30, SUM(total_amount), AVG(total_amount), COUNT(total_amount)
    FROM civicrm_contribution
    WHERE (receive_date BETWEEN @start AND @end) AND total_amount BETWEEN 10 AND 30;
SELECT 50, SUM(total_amount), AVG(total_amount), COUNT(total_amount)
    FROM civicrm_contribution
    WHERE (receive_date BETWEEN @start AND @end) AND total_amount BETWEEN 30 AND 50;
SELECT 100, SUM(total_amount), AVG(total_amount), COUNT(total_amount)
    FROM civicrm_contribution
    WHERE (receive_date BETWEEN @start AND @end) AND total_amount BETWEEN 50 AND 100;
SELECT 200, SUM(total_amount), AVG(total_amount), COUNT(total_amount)
    FROM civicrm_contribution
    WHERE (receive_date BETWEEN @start AND @end) AND total_amount BETWEEN 100 AND 200;
SELECT 1000, SUM(total_amount), AVG(total_amount), COUNT(total_amount)
    FROM civicrm_contribution
    WHERE (receive_date BETWEEN @start AND @end) AND total_amount BETWEEN 200 AND 1000;
SELECT 250000, SUM(total_amount), AVG(total_amount), COUNT(total_amount)
    FROM civicrm_contribution
    WHERE (receive_date BETWEEN @start AND @end) AND total_amount BETWEEN 1000 AND 250000;
SELECT 1000000, SUM(total_amount), AVG(total_amount), COUNT(total_amount)
    FROM civicrm_contribution
    WHERE (receive_date BETWEEN @start AND @end) AND total_amount BETWEEN 250000 AND 1000000;
SELECT ">1000000", SUM(total_amount), AVG(total_amount), COUNT(total_amount)
    FROM civicrm_contribution
    WHERE (receive_date BETWEEN @start AND @end) AND total_amount > 1000000;
