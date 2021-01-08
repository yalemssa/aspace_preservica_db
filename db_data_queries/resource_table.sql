SELECT resource.id
	, replace(replace(replace(replace(replace(identifier, ',', ''), '"', ''), ']', ''), '[', ''), 'null', '') AS call_number
	, resource.title as title
	, resource.publish
	, resource.suppressed
	, resource.create_time
	, resource.user_mtime
FROM resource
WHERE resource.repo_id = 12