CREATE TYPE dataset_lifecycle_stage AS ENUM('Unpublished', 'Published', 'Snapshotted');
CREATE TYPE unit AS ENUM('Unit');

CREATE TABLE global_log (
  id                BIGINT                   NOT NULL PRIMARY KEY, -- guaranteed to be contiguous and strictly increasing
  dataset_system_id BIGINT                   NOT NULL, -- Not REFERENCES because datasets can be deleted
  version           BIGINT                   NOT NULL,
  updated_at        TIMESTAMP WITH TIME ZONE NOT NULL,
  updated_by        VARCHAR(%USER_UID_LEN%)  NOT NULL
);

-- or this?
CREATE TABLE truth_manifest (
  id                BIGSERIAL                NOT NULL PRIMARY KEY,
  dataset_system_id BIGINT                   NOT NULL, -- Not REFERENCES because datasets can be deleted
  published_version BIGINT                   NULL, -- The last version before the creation of the current working copy (null if there is no working copy)
  version           BIGINT                   NOT NULL,
  updated_at        TIMESTAMP WITH TIME ZONE NOT NULL,
  dirty             BOOLEAN                  NOT NULL
);

CREATE TABLE table_map (
  system_id  BIGSERIAL                   NOT NULL PRIMARY KEY,
  dataset_id VARCHAR(%DATASET_ID_LEN%)   NOT NULL UNIQUE, -- This probably contains the domain ID in some manner...
  table_base VARCHAR(%PHYSTAB_BASE_LEN%) NOT NULL -- this + version_map's lifecycle_version is used to name per-dataset tables.
);

CREATE TABLE version_map (
  dataset_system_id     BIGINT                     NOT NULL REFERENCES table_map(system_id),
  lifecycle_version     BIGINT                     NOT NULL, -- this gets incremented per copy made.  It has nothing to do with the log's version
  lifecycle_stage       dataset_lifecycle_stage    NOT NULL,
  PRIMARY KEY (dataset_system_id, lifecycle_version)
);

CREATE TABLE column_map (
  system_id            BIGSERIAL                   NOT NULL,
  dataset_system_id    BIGINT                      NOT NULL REFERENCES table_map(system_id),
  lifecycle_version    BIGINT                      NOT NULL,
  logical_column       VARCHAR(%COLUMN_NAME_LEN%)  NOT NULL, -- "logical column" is roughly "user-visible SoQL name"
  type_name            VARCHAR(%TYPE_NAME_LEN%)    NOT NULL,
  physical_column_base VARCHAR(%PHYSCOL_BASE_LEN%) NOT NULL, -- "base" because some SoQL data types may require multiple physical columns
  is_primary_key       unit                        NULL, -- evil "unique" hack
  PRIMARY KEY (dataset_system_id, lifecycle_version, system_id),
  UNIQUE (dataset_system_id, lifecycle_version, logical_column),
  UNIQUE (dataset_system_id, lifecycle_version, is_primary_key), -- hack hack hack
  FOREIGN KEY (dataset_system_id, lifecycle_version) REFERENCES version_map (dataset_system_id, lifecycle_version) MATCH FULL
);
