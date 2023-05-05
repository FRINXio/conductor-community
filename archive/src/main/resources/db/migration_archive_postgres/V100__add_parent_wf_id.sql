ALTER TABLE archive.workflow_archive ADD COLUMN parent_workflow_id varchar(255);

update workflow_archive set parent_workflow_id=json_data::json ->> 'parentWorkflowId' where true;

CREATE INDEX IF NOT EXISTS workflow_archive_parent_workflow_id_idx ON archive.workflow_archive(parent_workflow_id ASC NULLS LAST);

DROP TABLE IF EXISTS public.workflow_archive, public.task_logs