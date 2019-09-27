package com.medium.hive.hook;

import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class CustomHook implements ExecuteWithHookContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomHook.class);
    private static final HashSet<String> OPERATION_NAMES = new HashSet<>();

    static {
        OPERATION_NAMES.add(HiveOperation.CREATETABLE.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERDATABASE.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERDATABASE_OWNER.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERTABLE_ADDCOLS.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERTABLE_LOCATION.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERTABLE_PROPERTIES.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERTABLE_RENAME.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERTABLE_RENAMECOL.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERTABLE_REPLACECOLS.getOperationName());
        OPERATION_NAMES.add(HiveOperation.CREATEDATABASE.getOperationName());
        OPERATION_NAMES.add(HiveOperation.DROPDATABASE.getOperationName());
        OPERATION_NAMES.add(HiveOperation.DROPTABLE.getOperationName());
    }

    @Override
    public void run(HookContext hookContext) throws Exception {
        assert (hookContext.getHookType() == HookType.POST_EXEC_HOOK);

        QueryPlan plan = hookContext.getQueryPlan();

        String operationName = plan.getOperationName();
        logWithHeader("Query executed: " + plan.getQueryString());
        logWithHeader("Operation: " + operationName);
        if (OPERATION_NAMES.contains(operationName)
                && !plan.isExplain()) {
            logWithHeader("Monitored Operation");
            Set<ReadEntity> inputs = hookContext.getInputs();
            Set<WriteEntity> outputs = hookContext.getOutputs();

            ObjectMapper objMapper = new ObjectMapper();

            logWithHeader("Hook metadata input values: " + objMapper.writeValueAsString(inputs));
            logWithHeader("Hook metadata output values: " + objMapper.writeValueAsString(outputs));

        } else {
            logWithHeader("Non-monitored Operation, ignoring hook");
        }
    }

    private void logWithHeader(Object obj){
        LOGGER.info("[CustomHook][Thread: "+Thread.currentThread().getName()+"] | " + obj);
    }
}