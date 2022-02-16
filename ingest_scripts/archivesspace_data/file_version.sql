SELECT fv.id 
	, dob.id as digital_object_id
	, dob.repo_id as repo_id
	, replace(fv.file_uri, 'https://preservica.library.yale.edu/api/entity/digitalFileContents/', '') as digital_file_id
	, fv.file_uri as file_uri
	, fv.file_size_bytes
	, ev.value as file_format
	, fv.create_time as file_version_create_time
	, fv.user_mtime as file_version_mtime
FROM file_version fv
JOIN digital_object dob on dob.id = fv.digital_object_id
LEFT JOIN enumeration_value ev on ev.id = fv.file_format_name_id
WHERE fv.file_uri like '%digitalFileContents%'