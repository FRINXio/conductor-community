ALTER TABLE archive.workflow_archive ADD COLUMN rbac_labels text[];

UPDATE archive.workflow_archive SET rbac_labels = ARRAY(
    SELECT json_array_elements_text(((json_data::json->'workflowDefinition'->>'description')::json)->'labels'));

CREATE INDEX IF NOT EXISTS workflow_archive_rbac_labels_idx ON archive.workflow_archive(rbac_labels ASC NULLS LAST);
