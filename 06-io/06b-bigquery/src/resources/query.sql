SELECT
  state_name,
  SUM(confirmed_cases) AS total_confirmed_case
FROM
  `bigquery-public-data.covid19_nyt.us_states`
GROUP BY
  1