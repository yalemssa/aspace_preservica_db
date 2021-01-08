SELECT hierarchies.id
	, hierarchies.root_record_id
	, hierarchies.repo_id
	, hierarchies.full_path
	, hierarchies.lvl
FROM hierarchies
WHERE hierarchies.repo_id = 12