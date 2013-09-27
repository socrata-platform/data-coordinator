CREATE TABLE IF NOT EXISTS %AUDIT_TABLE_NAME% (
  version    BIGINT                   NOT NULL, -- guaranteed to be contiguous and strictly increasing
  who        VARCHAR(%USER_UID_LEN%)  NOT NULL,
  at_time    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  PRIMARY KEY (version)
) %TABLESPACE%;

CREATE TABLE IF NOT EXISTS %TABLE_NAME% (
  version    BIGINT                   NOT NULL REFERENCES %AUDIT_TABLE_NAME% (version), -- guaranteed to be contiguous and strictly increasing
  subversion BIGINT                   NOT NULL, -- guaranteed to be contiguous and strictly increasing within a version
  what       VARCHAR(%OPERATION_LEN%) NOT NULL,
  aux        BYTEA                    NOT NULL,
  PRIMARY KEY (version, subversion)
) %TABLESPACE%;
