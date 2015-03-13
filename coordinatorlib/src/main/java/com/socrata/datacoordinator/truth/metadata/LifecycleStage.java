package com.socrata.datacoordinator.truth.metadata;

// Java-side mirror of the PostgreSQL dataset_lifecycle_stage
public enum LifecycleStage {
    Unpublished(com.socrata.datacoordinator.secondary.LifecycleStage.Unpublished),
    Published(com.socrata.datacoordinator.secondary.LifecycleStage.Published),
    Snapshotted(com.socrata.datacoordinator.secondary.LifecycleStage.Snapshotted),
    Discarded(com.socrata.datacoordinator.secondary.LifecycleStage.Discarded);

    public final com.socrata.datacoordinator.secondary.LifecycleStage correspondingSecondaryStage;

    LifecycleStage(com.socrata.datacoordinator.secondary.LifecycleStage correspondingSecondaryStage) {
        this.correspondingSecondaryStage = correspondingSecondaryStage;
    }
}
