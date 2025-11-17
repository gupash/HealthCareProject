from healthcare.utils import audit_log, get_spark_session, silver_loc

spark = get_spark_session("Adhoc Queries")

spark.read.format("delta").load(silver_loc).createGlobalTempView("health_partner_data")
spark.read.format("delta").load(audit_log).createGlobalTempView("audit_log")

spark.sql(
    """
select client_id, count(distinct(member_id)) as total_members, collect_set(member_id) as unique_members
from global_temp.health_partner_data group by client_id
"""
).show(truncate=False)

spark.sql(
    """
select zip5 from global_temp.health_partner_data where zip5 is not null
group by zip5 order by count(distinct(member_id)) desc limit 5
"""
).show(truncate=False)

spark.sql(
    """
select file_name, client_id, round((invalid_count * 1.0 * 100)/total_rows, 2) as ingestion_error_rate
from global_temp.audit_log
"""
).show(truncate=False)
