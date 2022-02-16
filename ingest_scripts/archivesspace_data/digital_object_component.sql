SELECT doc.id
	, doc.repo_id
	, doc.root_record_id
    , doc.parent_id
	, doc.component_id
	, doc.title
    , doc.publish
	, doc.create_time
    , doc.user_mtime
FROM digital_object_component doc
WHERE doc.repo_id = 12