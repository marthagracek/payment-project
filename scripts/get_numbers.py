import psycopg2

conn = psycopg2.connect(
    host='localhost',
    port=5432,
    user='postgres',
    password='postgres123',
    dbname='payment_analysis'
)

cur = conn.cursor()

print('QUERY 1: Average Monthly Revenue at Risk')
cur.execute('SELECT ROUND(AVG(monthly_revenue_at_risk), 0) FROM analytics.payment_failure_summary;')
result = cur.fetchone()
print(f'Average Monthly Revenue at Risk: ${result[0]:,.0f}')
print()

print('QUERY 2: Failure Rate by Payment Method')
cur.execute('SELECT payment_method, ROUND(AVG(failure_rate_pct), 1) FROM analytics.payment_failure_summary GROUP BY payment_method ORDER BY 2 DESC;')
results = cur.fetchall()
for row in results:
    print(f'{row[0]}: {row[1]}%')
print()

print('QUERY 3: Total Recovered Revenue')
cur.execute('SELECT ROUND(SUM(recovered_revenue), 0) FROM analytics.payment_failure_summary;')
result = cur.fetchone()
print(f'Total Recovered Revenue: ${result[0]:,.0f}')
print()

print('QUERY 4: Customer Risk Breakdown')
cur.execute('SELECT risk_segment, COUNT(*) FROM analytics.customer_risk_profile GROUP BY risk_segment ORDER BY 2 DESC;')
results = cur.fetchall()
for row in results:
    print(f'{row[0]}: {row[1]:,} customers')

cur.close()
conn.close()