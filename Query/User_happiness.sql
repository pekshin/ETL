-- самый счастливый user
SELECT
        country_code,
        name        ,
        location    ,
        tweet_text  ,
        SUM(text_sentiment) AS text_sentiment
FROM
        twitter_table
WHERE
        location IS NOT NULL
AND     location          <> ""
GROUP BY
        country_code,
        name        ,
        location    ,
        tweet_text
ORDER BY
        text_sentiment DESC limit 1
-- самый несчастный user
SELECT
        country_code,
        name        ,
        location    ,
        tweet_text  ,
        SUM(text_sentiment) AS text_sentiment
FROM
        twitter_table
WHERE
        location IS NOT NULL
AND     location          <> ""
GROUP BY
        country_code,
        name        ,
        location    ,
        tweet_text
ORDER BY
        text_sentiment limit 1