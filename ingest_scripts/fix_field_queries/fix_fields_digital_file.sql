UPDATE 
	digital_file
SET 
#	mimetype = CASE mimetype WHEN '' THEN NULL ELSE mimetype END,
    format_name = CASE format_name WHEN '' THEN NULL ELSE format_name END,
    format_puid = CASE format_puid WHEN '' THEN NULL ELSE format_puid END,
    working_path = CASE working_path WHEN '' THEN NULL ELSE working_path END