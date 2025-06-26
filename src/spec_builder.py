"""
Spec Builder - Creates JSON specifications from aggregated HL7 data
"""
import json
from collections import Counter
from hl7_fields import field_vocab, datatype_vocab, segment_vocab, field_datatype, field_description, infer_type

# Updated PHI fields list
PHI_FIELDS = {
    "PID.2.1", "PID.3.1", "PID.5", "PID.6", "PID.7", "PID.9", "PID.11.1", "PID.11.2", "PID.13.1", 
    "PID.13.6", "PID.13.7", "PID.14.1", "PID.14.6", "PID.14.7", "PID.18", "PID.19", "PID.20","PID.21","PID.23",
    "MRG.1.1", "MRG.4.1", "MRG.7", "IN.16", "GT1.2","GT1.3","GT1.5","GT1.6","GT1.7", "GT1.20",
    "NK1.2","NK1.4","NK1.5","NK1.6", "IN2.2","IN1.19","IN2.13","IN1.16"
}
PHI_PREFIXES = {f if f.count('.') > 0 else f + '.' for f in PHI_FIELDS}

def is_phi_field_local(segment, field_path):
    """Local PHI field check"""
    full_path = f"{segment}.{field_path}"
    return (
        full_path in PHI_FIELDS or
        any(full_path.startswith(prefix) for prefix in PHI_PREFIXES)
    )

def should_collect_unique_values(seg_name, field_path, values, hl7_type):
    """Determine if we should collect unique values for this field"""
    # Skip PHI fields
    if is_phi_field_local(seg_name, field_path):
        return False
    
    # Skip if too many unique values
    if "__TOO_MANY__" in values or len(values) > 100:
        return False
    
    # Skip TS (timestamp) fields - they're usually unique
    if hl7_type == "TS":
        return False
    
    # Skip SI (Set ID) fields - they're just sequential counters
    if hl7_type == "SI":
        return False

    return True

def key_sorter(k):
    """Sort field paths numerically"""
    import re
    return [int(part) if part.isdigit() else part for part in re.split(r'(\d+)', k)]

def create_individual_spec(msg_type, spec, presence, repeats, totals_by_type):
    """Create spec for a single message type"""
    if msg_type not in spec:
        return {}
    
    individual_spec = {}
    total = totals_by_type.get(msg_type, 0)
    
    for seg_name, fields in spec[msg_type].items():
        individual_spec[seg_name] = {"description": segment_vocab.get(seg_name, "")}
        
        for field_path, values in sorted(fields.items(), key=lambda item: key_sorter(item[0])):
            if field_path.startswith("_"):
                individual_spec[seg_name][field_path] = values
                continue

            total_count = sum(values.values())
            inferred_type = infer_type(values)
            hl7_type = field_datatype(seg_name, field_path)

            entry = {
                "description": field_description(seg_name, field_path),
                "hl7_type": hl7_type,
                "count": total_count,
                "present_in": presence[msg_type][seg_name].get(field_path, 0),
                "total": total,
                "type": inferred_type,
                "min_length": min(len(v) for v in values),
                "max_length": max(len(v) for v in values)
            }

            if should_collect_unique_values(seg_name, field_path, values, hl7_type):
                entry["unique_values"] = sorted([
                    {"value": v, "count": values[v], "percent": round(values[v]/total_count*100, 1)}
                    for v in values
                ], key=lambda x: x["count"], reverse=True)

            individual_spec[seg_name][field_path] = entry
    
    return individual_spec

def create_combined_spec(spec, presence, repeats, totals_by_type):
    """Merge all message types into a single combined spec"""
    combined = {}
    
    # Get all unique segments across all message types
    all_segments = set()
    for msg_segments in spec.values():
        all_segments.update(msg_segments.keys())
    
    for segment in all_segments:
        combined[segment] = {"description": segment_vocab.get(segment, "")}
        
        # Get all unique fields across all message types for this segment
        all_fields = set()
        for msg_type, msg_segments in spec.items():
            if segment in msg_segments:
                all_fields.update(msg_segments[segment].keys())
        
        # Handle repeating fields first
        repeating_fields = set()
        for msg_type, msg_repeats in repeats.items():
            if segment in msg_repeats:
                repeating_fields.update(msg_repeats[segment])
        
        # Merge field data across message types
        for field_path in sorted([f for f in all_fields if not f.startswith("_")], key=key_sorter):
            # Collect data from all message types that have this field
            merged_values = Counter()
            total_count = 0
            present_in = 0
            total_messages = sum(totals_by_type.values())
            
            for msg_type, msg_segments in spec.items():
                if segment in msg_segments and field_path in msg_segments[segment]:
                    field_data = msg_segments[segment][field_path]
                    merged_values.update(field_data)
                    total_count += sum(field_data.values())
                    present_in += presence[msg_type][segment].get(field_path, 0)
            
            if merged_values:
                # Create merged field entry
                inferred_type = infer_type(merged_values)
                hl7_type = field_datatype(segment, field_path)
                
                entry = {
                    "description": field_description(segment, field_path),
                    "hl7_type": hl7_type,
                    "count": total_count,
                    "present_in": present_in,
                    "total": total_messages,
                    "type": inferred_type,
                    "min_length": min(len(v) for v in merged_values),
                    "max_length": max(len(v) for v in merged_values)
                }

                if should_collect_unique_values(segment, field_path, merged_values, hl7_type):
                    entry["unique_values"] = sorted([
                        {"value": v, "count": merged_values[v], "percent": round(merged_values[v]/total_count*100, 1)}
                        for v in merged_values
                    ], key=lambda x: x["count"], reverse=True)

                combined[segment][field_path] = entry
        
        if repeating_fields:
            combined[segment]["_repeating_fields"] = sorted(list(repeating_fields), key=int)
    
    return combined