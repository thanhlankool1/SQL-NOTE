with t as (
        select asmp.service_name, asmp.agent_, asmp.type_, sum(case state when 2.0 then 1 else 0 end) + sum(case state when 6.0 then 1 else 0 end)  total, sum(case state when 2.0 then 1 else 0 end) as TOTAL_TC,
        sum(case state when 6.0 then 1 else 0 end) as TOTAL_TB,
        sum(case state when 1.0 then 1 else 0 end) as TOTAL_PEND,
        sum(case result
        when 1003.0 then 1
        when 1004.0 then 1
        when 103.0 then 1
        
        else 0
        end) as TB_EU,
        sum(case result

        when 600.0 then 1
        else 0
        end) as TB_CORE,
        top1, asmp.is_working_hour,asmp.monitor_id
        from OPERATOR_PROD.VH_AGENT_SERVICE_MONITOR_PROD_v2 asmp
        left join UMARKETADM.MS_ALL_TRANS_PAID_V3 ms3 on asmp.TYPE_ = ms3.TYPE_
        and asmp.AGENT_ = case when asmp.CREDIT_DEBIT = '+' then ms3.CREDITOR when asmp.CREDIT_DEBIT = '-' then ms3.DEBITOR end
        and ms3.created >= sysdate - 2/24/60 and ms3.created <= sysdate
        and ms3.TYPE_ in ('cardcashout','billpay', 'buy', 'bankcashout','sale','m4bpay', 'bankcashin','adjustment', 'transfer', 'bank-topup')
        where 1 = 1 and asmp.service_name is not null and sync = 1 and asmp.service_name = 'TRANSACTION_SPECIAL_GOOGLE'
        group by asmp.service_name, asmp.agent_, asmp.type_, top1, asmp.is_working_hour,asmp.monitor_id)
        select t.service_name, t.type_, top1, is_working_hour, monitor_id, sum(total) total, sum(total_tc) total_tc, (sum(total_tb) - sum(TB_CORE)) total_tb,sum(TOTAL_PEND) TOTAL_PEND, sum(TB_EU) TB_EU
        from t
        group by t.service_name, t.type_, top1, is_working_hour,monitor_id