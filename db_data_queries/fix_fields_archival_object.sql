UPDATE 
	archival_object
SET 
	parent_id = CASE parent_id WHEN 0 THEN NULL ELSE parent_id END,
    component_id = CASE component_id WHEN '' THEN NULL ELSE component_id END,
    preservica_collection_id = CASE preservica_collection_id WHEN '' THEN NULL ELSE preservica_collection_id END,
    extent = CASE extent WHEN '' THEN NULL ELSE extent END