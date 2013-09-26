package com.socrata.datacoordinator.truth.metadata;

import com.rojoma.json.ast.JString;
import com.rojoma.json.ast.JValue;
import com.rojoma.json.codec.JsonCodec;
import scala.Option;

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

    public static final JsonCodec<LifecycleStage> jCodec = new JsonCodec<LifecycleStage>() {
        @Override
        public JValue encode(LifecycleStage x) {
            return new JString(x.name());
        }

        @Override
        public Option<LifecycleStage> decode(JValue x) {
            if(x instanceof JString) {
                String s = ((JString)x).string();
                try {
                    return Option.apply(LifecycleStage.valueOf(s));
                } catch (IllegalArgumentException e) {
                    return Option.empty();
                }
            } else {
                return Option.empty();
            }
        }
    };
}
