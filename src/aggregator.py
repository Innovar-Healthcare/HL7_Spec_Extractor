from collections import defaultdict, Counter
from hl7_fields import field_datatype
from sequence_profiler import profile_sequences_by_message_type
import logging

logger = logging.getLogger(__name__)

MAX_UNIQUE = 100

class FieldStats:
    """Container for field statistics with memory optimization"""
    __slots__ = ['values', 'presence_count', '_frozen']
    
    def __init__(self):
        self.values = Counter()
        self.presence_count = 0
        self._frozen = False
    
    def add_value(self, value):
        if self._frozen:  # Skip processing if we already hit max
            return
            
        if len(self.values) < MAX_UNIQUE:
            self.values[value] += 1
        else:
            # Freeze this field to save memory and processing
            self.values = Counter({"__TOO_MANY__": 1})
            self._frozen = True
    
    def mark_present(self):
        self.presence_count += 1

def normalize_value(value):
    """Convert value to normalized string representation - optimized"""
    if isinstance(value, list):
        return "^".join(flatten_list(value)).strip("^")
    return str(value).strip()

def flatten_list(lst):
    """Recursively flatten nested lists to strings - optimized"""
    result = []
    for item in lst:
        if isinstance(item, list):
            result.extend(flatten_list(item))
        else:
            result.append(str(item).strip())
    return result

def collapse_complex_components(value):
    """Collapse complex field components (XCN, PPN, etc.) into a single string"""
    if isinstance(value, list):
        return "^".join([
            collapse_complex_components(v) if isinstance(v, list) else str(v).strip()
            for v in value
        ]).strip("^")
    return str(value).strip()

def process_field_value(value, field_path, stats, seen_fields, segment_name=""):
    """Process a single field value and update stats - optimized"""
    if stats._frozen:  # Skip if we already know there are too many values
        return
        
    normalized = normalize_value(value)
    
    # Create a unique key for this field in this message
    field_key = f"{segment_name}.{field_path}"
    
    # Skip completely empty values
    if not normalized:
        return
        
    # Mark field as present only once per message
    if field_key not in seen_fields:
        stats.mark_present()
        seen_fields.add(field_key)
    
    stats.add_value(normalized)

# Cache for datatype lookups to avoid repeated dictionary access
_datatype_cache = {}

def is_complex_type(hl7_type):
    """Check if an HL7 type has components defined - with caching"""
    if hl7_type in _datatype_cache:
        return _datatype_cache[hl7_type]
    
    try:
        from hl7_fields import datatype_vocab
        result = hl7_type in datatype_vocab and isinstance(datatype_vocab.get(hl7_type), dict)
    except ImportError:
        # Fallback to hardcoded list if datatype_vocab not available
        result = hl7_type in ["CX", "XCN", "PPN", "XAD", "XTN", "XPN", "EI", "CE", "CWE", "HD", "PL", "MSG", "PT", "VID", "TS", "TQ", "MOC", "CP", "CQ", "FC", "DR", "LA1", "LA2"]
    
    _datatype_cache[hl7_type] = result
    return result

def process_field_unified(field_value, base_path, field_stats, msg_type, seg_name, seen_fields, is_repeating=False):
    """Unified field processing for both repeating and non-repeating fields - optimized"""
    # Cache the datatype lookup
    cache_key = f"{seg_name}.{base_path}"
    if cache_key not in _datatype_cache:
        _datatype_cache[cache_key] = field_datatype(seg_name, base_path)
    hl7_type = _datatype_cache[cache_key]
    
    # Special handling for XCN/PPN non-repeating fields (keep existing behavior)
    if hl7_type in ["XCN", "PPN"] and not is_repeating:
        stats = field_stats[msg_type][seg_name][base_path]
        process_complex_field_collapsed(field_value, base_path, stats, seen_fields, seg_name)
        return
    
    # Determine if we should break into components
    should_break_components = is_complex_type(hl7_type)
    
    # Handle repetitions
    if is_repeating:
        repetitions = field_value if isinstance(field_value, list) else [field_value]
    else:
        repetitions = [field_value]
    
    for repetition in repetitions:
        if should_break_components and isinstance(repetition, list):
            # Break into components
            for i, component in enumerate(repetition):
                if component or str(component).strip():  # Skip empty components
                    component_path = f"{base_path}.{i + 1}"
                    component_stats = field_stats[msg_type][seg_name][component_path]
                    process_field_value(component, component_path, component_stats, seen_fields, seg_name)
        elif should_break_components and not isinstance(repetition, list):
            # Single value that should be treated as component .1
            if repetition or str(repetition).strip():
                component_path = f"{base_path}.1"
                component_stats = field_stats[msg_type][seg_name][component_path]
                process_field_value(repetition, component_path, component_stats, seen_fields, seg_name)
        else:
            # Simple field - process as whole value
            stats = field_stats[msg_type][seg_name][base_path]
            process_field_value(repetition, base_path, stats, seen_fields, seg_name)

def process_complex_field_collapsed(value, field_path, stats, seen_fields, segment_name=""):
    """Process complex fields (XCN, PPN, etc.) with component collapsing - kept for backward compatibility"""
    if isinstance(value, list) and all(isinstance(r, list) for r in value):
        # Multiple repetitions, each with components
        for repeat in value:
            collapsed = collapse_complex_components(repeat)
            if collapsed:
                process_field_value(collapsed, field_path, stats, seen_fields, segment_name)
    else:
        # Single repetition
        collapsed = collapse_complex_components(value)
        if collapsed:
            process_field_value(collapsed, field_path, stats, seen_fields, segment_name)

def consolidate_single_component_fields(spec, presence):
    """Consolidate single-component fields - optimized"""
    logger.info("Consolidating single-component fields...")
    
    for msg_type, segments in spec.items():
        for seg_name, fields in list(segments.items()):
            field_components = {}
            for field_path in fields:
                base_field = field_path.split('.')[0]
                if base_field not in field_components:
                    field_components[base_field] = set()
                field_components[base_field].add(field_path)
            
            for base_field, paths in field_components.items():
                # Use cached datatype lookup
                cache_key = f"{seg_name}.{base_field}"
                if cache_key not in _datatype_cache:
                    _datatype_cache[cache_key] = field_datatype(seg_name, base_field)
                hl7_type = _datatype_cache[cache_key]
                
                # Skip consolidation for complex types that we handle specially
                if hl7_type in ["XCN", "PPN", "CX", "XAD", "XTN", "XPN"]:
                    continue
                
                # Only consolidate if there's exactly one component ending in .1
                if len(paths) == 1 and next(iter(paths)).endswith(".1"):
                    component_path = next(iter(paths))
                    # Make sure there's actually data before consolidating
                    if component_path in fields and fields[component_path]:
                        fields[base_field] = fields.pop(component_path)
                        presence[msg_type][seg_name][base_field] = presence[msg_type][seg_name].pop(component_path)
    
    return spec, presence

def aggregate_data(messages):
    """Main aggregation function - optimized"""
    logger.info(f"Starting aggregation of {len(messages)} messages...")
    
    field_stats = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: FieldStats())))
    repeat_tracker = defaultdict(lambda: defaultdict(set))
    totals_by_type = defaultdict(int)
    
    # Process messages with progress logging
    processed_count = 0
    log_interval = max(1, len(messages) // 10)  # Log every 10%
    
    for msg in messages:
        if msg is None:
            continue
            
        seen_fields = set()  # Reset for each message
        msg_type = msg["message_type"]
        totals_by_type[msg_type] += 1
        
        # Process each segment
        for segment in msg["segments"]:
            seg_name = segment["name"]
            fields = segment["fields"]
            repeating_fields = segment.get("repeating_fields", set())
            
            for field_index, field_value in enumerate(fields, start=1):
                base_path = str(field_index)
                
                # Track repeating fields
                if base_path in repeating_fields:
                    repeat_tracker[msg_type][seg_name].add(base_path)
                
                # SIMPLIFIED PROCESSING LOGIC
                if base_path in repeating_fields:
                    # Process as repeating field
                    process_field_unified(field_value, base_path, field_stats, msg_type, seg_name, seen_fields, is_repeating=True)
                else:
                    # Process as non-repeating field
                    process_field_unified(field_value, base_path, field_stats, msg_type, seg_name, seen_fields, is_repeating=False)
        
        processed_count += 1
        if processed_count % log_interval == 0 or processed_count == len(messages):
            progress = (processed_count / len(messages)) * 100
            logger.info(f"Processed {processed_count}/{len(messages)} messages ({progress:.1f}%)")
    
    logger.info("Converting field statistics to output format...")
    
    # Convert to expected output format
    spec = defaultdict(lambda: defaultdict(lambda: defaultdict(Counter)))
    presence = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    
    for msg_type in field_stats:
        for seg_name in field_stats[msg_type]:
            for field_path in field_stats[msg_type][seg_name]:
                stats = field_stats[msg_type][seg_name][field_path]
                # Only include fields that actually have non-empty values and presence > 0
                if stats.values and stats.presence_count > 0:
                    # Additional check: make sure we have actual content, not just empty strings
                    has_content = any(value.strip() for value in stats.values.keys() if value != "__TOO_MANY__")
                    if has_content:
                        spec[msg_type][seg_name][field_path] = stats.values
                        presence[msg_type][seg_name][field_path] = stats.presence_count
    
    # Apply consolidation logic
    spec, presence = consolidate_single_component_fields(spec, presence)
    
    # Add sequence profiling by message type
    logger.info("Profiling sequences by message type...")
    sequence_profiles = profile_sequences_by_message_type(messages)
    
    logger.info("Aggregation completed successfully")
    
    return {
        "spec": spec,
        "repeats": repeat_tracker,
        "presence": presence,
        "total": len(messages),
        "totals_by_type": dict(totals_by_type),
        "sequence_profiles_by_type": sequence_profiles
    }