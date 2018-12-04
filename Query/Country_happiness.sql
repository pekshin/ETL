---- что-то одно отсюда необходимо использовать, блоки разделены: --*
--самая счастливая по country_code: "US" "23"
SELECT
        country_code,
        SUM(text_sentiment) AS text_sentiment
FROM
        twitter_table
WHERE
        country_code IS NOT NULL
AND     country_code          <> "None"
GROUP BY
        country_code
ORDER BY
        text_sentiment DESC limit 1
-- самая счастливая страна == USA # по локации
SELECT
        location,
        SUM(text_sentiment) AS text_sentiment
FROM
        twitter_table
WHERE
        location IS NOT NULL
AND     location          <> ""
GROUP BY
        location
ORDER BY
        text_sentiment DESC limit 1
-- самая несчастная == "PT" "-5"  # по country_code
SELECT
        country_code,
        SUM(text_sentiment) AS text_sentiment
FROM
        twitter_table
WHERE
        country_code IS NOT NULL
AND     country_code          <> "None"
GROUP BY
        country_code
ORDER BY
        text_sentiment limit 1
-- самая несчастная == London # по локации
SELECT
        location,
        SUM(text_sentiment) AS text_sentiment
FROM
        twitter_table
WHERE
        location IS NOT NULL
AND     location          <> ""
GROUP BY
        location
ORDER BY
        text_sentiment limit 1