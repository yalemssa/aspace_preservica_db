#This query from the AS database populates the archival object table
SELECT ao.id
	, ao.repo_id
	, ao.root_record_id
	, ao.parent_id
	, ao.ref_id
	, ao.component_id
	, replace(ao.display_string, '"', "'") as title
	, ao.publish
	, ev.value as level
	, ANY_VALUE(pres_notes.collection_id) as preservica_collection_id
    , GROUP_CONCAT(DISTINCT CONCAT(IF(extent.number is not NULL, extent.number, ""), ' ', IF(ev1.value is not NULL, ev1.value,""))) as extent
	, GROUP_CONCAT(DISTINCT CONCAT(IF(ev2.value IS NOT NULL, ev2.value, '')
                                                        , ' '
                                                        , IF(tc.indicator IS NOT NULL, tc.indicator, '')
                                                        , ' ['
                                                        , IF(cp.name IS NOT NULL, cp.name, '')
                                                        , '], '
                                                        , IF(ev3.value IS NOT NULL, ev3.value, '')
                                                        , ' '
                                                        , IF(sc.indicator_2 IS NOT NULL, sc.indicator_2, 'NULL'))
						ORDER BY CAST(tc.indicator as UNSIGNED), CAST(sc.indicator_2 as UNSIGNED)
						SEPARATOR '; ') as physical_containers
	, ao.create_time
	, ao.user_mtime as mtime
FROM archival_object ao
LEFT JOIN enumeration_value ev on ev.id = ao.level_id
LEFT JOIN (SELECT note.archival_object_id as ao_id
				, replace(JSON_UNQUOTE(JSON_EXTRACT(CAST(CONVERT(note.notes using utf8) as json), '$.subnotes[0].content')), 
					'https://preservica.library.yale.edu/explorer/explorer.html#prop:4&',
					'') as collection_id
			FROM note
			WHERE note.notes like '%preservica%'
			AND note.notes like '%otherfindaid%') as pres_notes on pres_notes.ao_id = ao.id
LEFT JOIN extent on extent.archival_object_id = ao.id
LEFT JOIN enumeration_value ev1 on ev1.id = extent.extent_type_id
LEFT JOIN instance on instance.archival_object_id = ao.id
LEFT JOIN sub_container sc on sc.instance_id = instance.id
LEFT JOIN top_container_link_rlshp tclr on tclr.sub_container_id = sc.id
LEFT JOIN top_container tc on tc.id = tclr.top_container_id
LEFT JOIN top_container_profile_rlshp tcpr on tcpr.top_container_id = tc.id
LEFT JOIN container_profile cp on tcpr.container_profile_id = cp.id
LEFT JOIN enumeration_value ev2 on ev2.id = tc.type_id
LEFT JOIN enumeration_value ev3 on ev3.id = sc.type_2_id
WHERE ao.repo_id = 12
AND (pres_notes.collection_id is NULL or pres_notes.collection_id != 'b4732c04-8bf4-4894-9111-f86a090997f4')
GROUP BY ao.id
#actually maybe not right, since we can't guarantee that every archival object will have this
#AND pres_notes.collection_id is not null



#add extents, physical instance info