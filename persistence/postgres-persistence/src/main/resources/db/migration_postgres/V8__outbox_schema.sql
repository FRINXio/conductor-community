CREATE TABLE outbox_table (
  ID varchar(255) NOT NULL,
  aggregateType varchar(255) NOT NULL,
  aggregateId varchar(255) NOT NULL,
  payload TEXT,
  eventType varchar(255) NOT NULL,
  PRIMARY KEY (ID, aggregateType)
);