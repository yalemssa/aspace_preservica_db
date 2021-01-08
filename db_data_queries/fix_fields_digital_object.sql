SELECT ao.id as archival_object_id
	, resource.call_number
    , resource.title
	, hierarchies.full_path as hierarchy
    , hierarchies.lvl as depth
	, ao.title
    , ao.extent
    , ao.physical_containers
    , GROUP_CONCAT(DISTINCT du.coverage_from)
	, GROUP_CONCAT(DISTINCT du.coverage_to)
    , GROUP_CONCAT(DISTINCT du.security_tag) 
    , GROUP_CONCAT(DISTINCT dob.title) as dig_ob_title
    , ANY_VALUE(manifestation.typeref) as typeref
    , GROUP_CONCAT(digital_file.id SEPARATOR '; ') as concated_links
FROM digital_file
JOIN manifestation on manifestation.id = digital_file.manifestation_id
JOIN deliverable_unit du on du.id = manifestation.deliverable_unit_id
LEFT JOIN digital_object dob on dob.digital_object_id = du.id
LEFT JOIN archival_object ao on ao.id = dob.archival_object_id
LEFT JOIN hierarchies on hierarchies.id = ao.id
LEFT JOIN resource on resource.id = ao.root_record_id
GROUP BY ao.id