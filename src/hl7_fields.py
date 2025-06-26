
import json
import re
from uuid import UUID

with open("schemas/hl7_segments.json") as f:
    segment_vocab = json.load(f)
with open("schemas/hl7_fields.json") as f:
    field_vocab = json.load(f)
with open("schemas/hl7_datatypes.json") as f:
    datatype_vocab = json.load(f)

PHI_FIELDS = {
    "PID.2.1", "PID.3.1", "PID.5", "PID.7", "PID.11.1", "PID.11.2", "PID.18", "PID.19"
}
PHI_PREFIXES = {f if f.count('.') > 0 else f + '.' for f in PHI_FIELDS}

def is_phi_field(segment, field_path):
    full_path = f"{segment}.{field_path}"
    return (
        full_path in PHI_FIELDS or
        any(full_path.startswith(prefix) for prefix in PHI_PREFIXES)
    )

def infer_type(values):
    sample = next((v for v in values if v != "__TOO_MANY__"), None)
    if not sample:
        return "unknown"
    if re.fullmatch(r"\d{8}", sample):
        return "yyyyMMdd"
    if re.fullmatch(r"\d{14}", sample):
        return "yyyyMMddHHmmss"
    if re.fullmatch(r"\d{12}", sample):
        return "yyyyMMddHHmm"
    try:
        UUID(sample)
        return "uuid"
    except:
        pass
    if re.fullmatch(r"[-+]?\d+", sample):
        return "int"
    if re.fullmatch(r"[-+]?\d*\.\d+", sample):
        return "float"
    if sample.lower() in {"true", "false", "y", "n"}:
        return "boolean"
    return "string"

def field_description(segment, field_path):
    parts = field_path.split(".")
    field_key = f"{segment}.{parts[0]}"
    field_info = field_vocab.get(field_key)

    if not field_info:
        return ""

    field_name = field_info.get("field_name", "")
    field_type = field_info.get("field_type")

    simple_types = {"ST", "ID", "DT", "TM", "TX", "NM", "SI", "FT", "IS"}
    if field_type in simple_types or len(parts) == 1:
        return field_name

    datatype_info = datatype_vocab.get(field_type)
    component_info = datatype_info.get(parts[1]) if datatype_info else None
    component_name = component_info.get("name") if component_info else ""

    if component_name:
        return f"{field_name} - {component_name}"
    return field_name

def field_datatype(segment, field_path):
    return field_vocab.get(f"{segment}.{field_path.split('.')[0]}", {}).get("field_type", "")
