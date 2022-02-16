SELECT event.id
	, event.repo_id  
    , elr.archival_object_id
    , ev3.value as event_link_role  
	, ev.value as event_type     
    , ev2.value as event_outcome     
    , event.outcome_note     
    , event.timestamp
    , event.refid  
    , agent_auth.sort_name
    , agent_auth.agent_id
    , agent_auth.agent_role
    , event.create_time     
    , event.user_mtime     
FROM event_link_rlshp elr 
JOIN event on event.id = elr.event_id 
LEFT JOIN enumeration_value ev on ev.id = event.event_type_id 
LEFT JOIN enumeration_value ev2 on ev2.id = event.outcome_id 
LEFT JOIN enumeration_value ev3 on ev3.id = elr.role_id 
LEFT JOIN (SELECT event.id
        , GROUP_CONCAT(ev.value SEPARATOR '; ') as agent_role
        , GROUP_CONCAT(np.agent_person_id SEPARATOR '; ') as agent_id
        , GROUP_CONCAT(np.sort_name SEPARATOR '; ') as sort_name
    FROM linked_agents_rlshp lar
    JOIN event on event.id = lar.event_id
    JOIN name_person np on np.agent_person_id = lar.agent_person_id
    LEFT JOIN enumeration_value ev on ev.id = lar.role_id
    WHERE ev.value in ('implementer', 'authorizer')
    AND event.repo_id = 12
    AND np.is_display_name is not null
    GROUP BY event.id
    UNION ALL
    SELECT event.id
        , GROUP_CONCAT(ev.value SEPARATOR '; ') as agent_role
        , GROUP_CONCAT(nce.agent_corporate_entity_id SEPARATOR '; ') as agent_id
        , GROUP_CONCAT(nce.sort_name SEPARATOR '; ') as sort_name
    FROM linked_agents_rlshp lar
    JOIN event on event.id = lar.event_id
    JOIN name_corporate_entity nce on nce.agent_corporate_entity_id = lar.agent_corporate_entity_id
    LEFT JOIN enumeration_value ev on ev.id = lar.role_id
    WHERE ev.value in ('implementer', 'authorizer')
    AND event.repo_id = 12
    AND nce.is_display_name is not null
    GROUP BY event.id) as agent_auth on agent_auth.id = event.id
WHERE elr.archival_object_id is not null
AND event.repo_id = 12
AND ev.value not in ('component_transfer', 'acknowledgement_sent')