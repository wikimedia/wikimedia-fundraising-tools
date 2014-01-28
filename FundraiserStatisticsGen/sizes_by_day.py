#!/usr/bin/env python

# FIXME: dayoffiscalyear
start_time = "20120701"
end_time = "20130701"
#start_time = "20060101"
#end_time = "20140101"
ranges = [
    [0, 10],
    [10,30],
    [30,50],
    [50,100],
    [100,200],
    [200,1000],
    [1000,2500],
    [2500,10000],
    [10000,1000000000]
]

amount_slices_cols = ", ".join([
    """
        SUM(
            IF(
                total_amount > {min} AND total_amount <= {max},
                total_amount, 0)
        ) AS total_{min}_{max},
        SUM(
            IF(
                total_amount > {min} AND total_amount <= {max},
                1, 0)
        ) AS num_{min}_{max}
    """.format(
        min=min_amount,
        max=max_amount
    ) for min_amount, max_amount in ranges
])

sum_query = """
    SELECT
        FROM_DAYS(TO_DAYS(receive_date)) AS day,
        {amount_slices_cols}
    FROM
        civicrm_contribution
    WHERE
        receive_date > {begin}
        AND receive_date <= {end}
    GROUP BY
        YEAR(receive_date), DAYOFYEAR(receive_date)
    ORDER BY
        YEAR(receive_date), DAYOFYEAR(receive_date)
        ASC
""".format(
    begin=start_time,
    end=end_time,
    amount_slices_cols=amount_slices_cols
)
print sum_query
