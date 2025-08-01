/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.optimizer.rules.PlanConsistencyChecker;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

public final class LogicalVerifier extends PostOptimizationPhasePlanVerifier<LogicalPlan> {

    public static final LogicalVerifier INSTANCE = new LogicalVerifier();

    private LogicalVerifier() {}

    @Override
    boolean skipVerification(LogicalPlan optimizedPlan, boolean skipRemoteEnrichVerification) {
        if (skipRemoteEnrichVerification) {
            // AwaitsFix https://github.com/elastic/elasticsearch/issues/118531
            var enriches = optimizedPlan.collectFirstChildren(Enrich.class::isInstance);
            if (enriches.isEmpty() == false && ((Enrich) enriches.get(0)).mode() == Enrich.Mode.REMOTE) {
                return true;
            }
        }
        return false;
    }

    @Override
    void checkPlanConsistency(LogicalPlan optimizedPlan, Failures failures, Failures depFailures) {
        optimizedPlan.forEachUp(p -> {
            PlanConsistencyChecker.checkPlan(p, depFailures);

            if (failures.hasFailures() == false) {
                if (p instanceof PostOptimizationVerificationAware pova) {
                    pova.postOptimizationVerification(failures);
                }
                p.forEachExpression(ex -> {
                    if (ex instanceof PostOptimizationVerificationAware va) {
                        va.postOptimizationVerification(failures);
                    }
                });
            }
        });
    }
}
