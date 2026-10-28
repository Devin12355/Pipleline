package com.devin.pipeline.core.control;

/**
 * Decision taken after a stage threw: keep going or stop the pipeline.
 */
public enum ErrorAction {

    /** Record the failure and move on to the next stage. */
    CONTINUE,

    /** Record the failure and stop the pipeline with state {@code FAILED}. */
    ABORT
}
