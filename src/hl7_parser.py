import hl7

def parse_hl7_message(raw_msg):
    segments = []

    # Normalize segment delimiters
    raw_msg = raw_msg.replace('\n', '\r').strip()

    try:
        message = hl7.parse(raw_msg)
    except Exception as e:
        print(f"Failed to parse message: {e}")
        return None

    for segment in message:
        seg_name = segment[0][0] if isinstance(segment[0], list) else segment[0]
        fields = []
        repeating_fields = set()

        for field_index in range(1, len(segment)):  # skip segment name
            field = segment[field_index]

            # Handle multiple repetitions
            is_repeating = isinstance(field, list) and len(field) > 1
            if is_repeating:
                repeating_fields.add(str(field_index))
                repetitions = field
            else:
                repetitions = [field]

            parsed_repeats = []

            for repetition in repetitions:
                if not isinstance(repetition, list):
                    parsed_repeats.append(str(repetition))
                    continue

                parsed_components = []
                for component in repetition:
                    if isinstance(component, list):
                        subcomps = [str(sub) for sub in component]
                        parsed_components.append(subcomps)
                    else:
                        parsed_components.append(str(component))

                parsed_repeats.append(parsed_components if len(parsed_components) > 1 else parsed_components[0])

            # Collapse non-repeating fields
            parsed_field = parsed_repeats if len(parsed_repeats) > 1 else parsed_repeats[0]
            fields.append(parsed_field)

        segments.append({
            "name": seg_name,
            "fields": fields,
            "repeating_fields": repeating_fields
        })

    # Extract message type from MSH-9
    try:
        msh_segment = [s for s in segments if s["name"] == "MSH"][0]
        msh_9 = msh_segment["fields"][8]
        message_type = "^".join(msh_9) if isinstance(msh_9, list) else msh_9
    except Exception:
        message_type = "UNKNOWN"

    return {"message_type": message_type, "segments": segments}
