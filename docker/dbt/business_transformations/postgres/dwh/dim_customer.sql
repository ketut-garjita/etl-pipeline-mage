{{ config(materialized='view') }}

WITH cleaned AS (
SELECT 
    Customer_Id AS Customer_Id,
    LOWER(Customer_Email) AS Customer_Email,
    UPPER(LEFT(Customer_Fname, 1)) || LOWER(SUBSTR(Customer_Fname, 2)) AS Customer_Fname,
    UPPER(LEFT(Customer_Lname, 1)) || LOWER(SUBSTR(Customer_Lname, 2)) AS Customer_Lname,
    Customer_Segment AS Customer_Segment,
    Customer_City AS Customer_City,
    Customer_Country AS Customer_Country,
    Customer_State AS Customer_State,
    Customer_Street AS Customer_Street,
    Customer_Zipcode AS Customer_Zipcode
FROM {{ ref('stg_customer') }}
WHERE Customer_Id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
    PARTITION BY (
	Customer_Id, 
	Customer_Email,
	Customer_Fname,
	Customer_Lname,
	Customer_Segment,
	Customer_City,
	Customer_Country,
	Customer_State,
	Customer_Street,
	Customer_Zipcode)
	ORDER BY Customer_Id) = 1
    )

SELECT * FROM cleaned

