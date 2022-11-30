-- Databricks notebook source
-- MAGIC %python
-- MAGIC r_cte_seed_df = spark.sql("""
-- MAGIC SELECT
-- MAGIC          svc_no
-- MAGIC         ,vchr_vl
-- MAGIC         ,CAST( TRIM(no_of_occr) || ' x $' || TRIM(vchr_vl) AS VARCHAR(255)) AS all_denom 
-- MAGIC         ,rno
-- MAGIC     FROM EP_SSOT_TEMP_TECH_VIEW.RCHG_DENM   
-- MAGIC     WHERE rno = 1
-- MAGIC  """
-- MAGIC )
-- MAGIC 
-- MAGIC r_cte_seed_df.createOrReplaceTempView("denom_conc")
-- MAGIC 
-- MAGIC while True:
-- MAGIC     recursive_df = spark.sql("""
-- MAGIC      SELECT 
-- MAGIC          bse.svc_no                       
-- MAGIC         ,bse.vchr_vl
-- MAGIC         ,TRIM(all_denom) ||' & ' || TRIM(bse.no_of_occr) || ' x $' || TRIM(bse.vchr_vl) AS all_denom
-- MAGIC         ,bse.rno
-- MAGIC     FROM denom_conc   rcr
-- MAGIC     
-- MAGIC     LEFT OUTER JOIN EP_SSOT_TEMP_TECH_VIEW.RCHG_DENM bse 
-- MAGIC     ON bse.svc_no = rcr.svc_no
-- MAGIC     AND bse.rno = rcr.rno+1
-- MAGIC     
-- MAGIC     WHERE bse.svc_no IS NOT NULL 
-- MAGIC     """)
-- MAGIC     recursive_df.createOrReplaceTempView("denom_conc")
-- MAGIC     r_cte_seed_df = r_cte_seed_df.union(recursive_df)
-- MAGIC     if recursive_df.count() == 0:
-- MAGIC         r_cte_seed_df.createOrReplaceTempView("denom_conc")
-- MAGIC         break
-- MAGIC     else:
-- MAGIC         continue 

-- COMMAND ----------

INSERT INTO EP_SSOT_TEMP.TEMP_DENM 
(
     svc_no 
    ,vchr_vl 
    ,all_denom 
    ,rno 

)
SELECT 
     svc_no 
    ,vchr_vl 
    ,all_denom 
    ,rno 
FROM denom_conc;
