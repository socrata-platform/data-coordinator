CREATE TYPE dataset_lifecycle_stage AS ENUM('Unpublished', 'Published', 'Snapshotted');

CREATE TABLE global_log (
  id         BIGINT                    NOT NULL PRIMARY KEY, -- guaranteed to be contiguous and strictly increasing
  dataset_id VARCHAR(%DATASET_ID_LEN%) NOT NULL, -- Hm, maybe the dataset system_id instead?
  version    BIGINT                    NOT NULL,
  updated_at TIMESTAMP WITH TIME ZONE  NOT NULL,
  updated_by VARCHAR(%USER_UID_LEN%)   NOT NULL
);

CREATE TABLE table_map (
  system_id  BIGSERIAL                   NOT NULL PRIMARY KEY,
  dataset_id VARCHAR(%DATASET_ID_LEN%)   NOT NULL UNIQUE,
  table_base VARCHAR(%PHYSTAB_BASE_LEN%) NOT NULL -- this + version_map's lifecycle_version is used to name per-dataset tables.
);

CREATE TABLE version_map (
  dataset_system_id BIGINT                     NOT NULL REFERENCES table_map(system_id),
  lifecycle_version BIGINT                     NOT NULL, -- this gets incremented per copy made; it has nothing to do with the log's version
  lifecycle_stage   dataset_lifecycle_stage    NOT NULL,
  primary_key       VARCHAR(%COLUMN_NAME_LEN%) NULL,
  PRIMARY KEY (dataset_id, lifecycle_version)
);

CREATE TABLE column_map (
  dataset_system_id    BIGINT                      NOT NULL REFERENCES table_map(system_id),
  lifecycle_version    BIGINT                      NOT NULL,
  logical_column       VARCHAR(%COLUMN_NAME_LEN%)  NOT NULL, -- "logical column" is roughly "user-visible SoQL name"
  type_name            VARCHAR(%TYPE_NAME_LEN%)    NOT NULL,
  physical_column_base VARCHAR(%PHYSCOL_BASE_LEN%) NOT NULL, -- "base" because some SoQL data types may require multiple physical columns
  PRIMARY KEY (dataset_id, lifecycle_version, logical_column),
  FOREIGN KEY (dataset_id, lifecycle_version) REFERENCES version_map (dataset_id, lifecycle_version) MATCH FULL
);
