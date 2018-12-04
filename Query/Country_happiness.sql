---- ���-�� ���� ������ ���������� ������������, ����� ���������: --*
--����� ���������� �� country_code: "US" "23"
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
-- ����� ���������� ������ == USA # �� �������
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
-- ����� ���������� == "PT" "-5"  # �� country_code
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
-- ����� ���������� == London # �� �������
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