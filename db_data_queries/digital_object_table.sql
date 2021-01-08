SELECT do.id
	, replace(do.digital_object_id, '	', '') as digital_object_id
	, GROUP_CONCAT(DISTINCT ao.id SEPARATOR '; ') as archival_object_id
	, do.title as title
	, do.publish
	, IF(GROUP_CONCAT(fv.file_uri) like '%digitalFileContents%', 'Y', 'N') as has_content_link
	, do.create_time
	, do.user_mtime
FROM digital_object do
LEFT JOIN file_version fv on fv.digital_object_id = do.id
LEFT JOIN instance_do_link_rlshp idlr on idlr.digital_object_id = do.id
LEFT JOIN instance on idlr.instance_id = instance.id
LEFT JOIN archival_object ao on instance.archival_object_id = ao.id
WHERE do.repo_id = 12
AND (do.digital_object_id not like '%digcoll%'
	AND do.digital_object_id not like '%130.132%'
	AND do.digital_object_id not like '%ms_%'
	AND do.digital_object_id not like '%ru_%'
	AND do.digital_object_id not like '%handle%'
	AND do.digital_object_id not like '%aviary%'
	AND do.digital_object_id not like '%Digital Object ID%'
	AND do.digital_object_id not like '%to_delete%'
	AND do.digital_object_id not like '%11110101%'
	AND do.digital_object_id not like '%2005-A-026-0001.iso%')
GROUP BY do.id