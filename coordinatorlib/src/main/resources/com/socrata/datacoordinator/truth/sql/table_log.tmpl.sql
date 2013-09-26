CREATE TABLE IF NOT EXISTS %TABLE_NAME% (
  version    BIGINT                   NOT NULL, -- guaranteed to be contiguous and strictly increasing
  subversion BIGINT                   NOT NULL, -- guaranteed to be contiguous and strictly increasing within a version
  what       VARCHAR(%OPERATION_LEN%) NOT NULL,
  aux        BYTEA                    NOT NULL,
  PRIMARY KEY (version, subversion)
) %TABLESPACE%;
