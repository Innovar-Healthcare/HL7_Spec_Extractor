from collections import Counter, defaultdict

def normalize_sequence(seq):
    normalized = []
    for segment in seq:
        if not normalized:
            normalized.append(segment)
        elif segment == normalized[-1].rstrip('+'):
            # Repeat of previous segment
            if not normalized[-1].endswith('+'):
                normalized[-1] += '+'
        else:
            normalized.append(segment)
    return normalized

def profile_sequences_by_message_type(messages):
    """Profile sequences separately for each message type"""
    sequence_data = {}
    
    # Group messages by type
    messages_by_type = defaultdict(list)
    for msg in messages:
        if msg is None:
            continue
        msg_type = msg["message_type"]
        messages_by_type[msg_type].append(msg)
    
    # Process each message type separately
    for msg_type, msgs in messages_by_type.items():
        sequence_data[msg_type] = profile_sequences_for_type(msgs)
    
    return sequence_data

def profile_sequences_for_type(messages):
    """Profile sequences for a single message type with pattern merging"""
    sequence_counts = Counter()
    segments_presence = defaultdict(lambda: {"present_in": 0, "repeats": False})
    total_messages = len(messages)

    canonical_sequences = []

    # Track segment presence per message (once per message)
    for msg in messages:
        if msg is None:
            continue

        segment_order = []
        segment_occurrences = Counter()

        # Track unique segments in this message (for presence counting)
        unique_segments_this_message = set()

        for segment in msg["segments"]:
            seg_name = segment["name"]
            segment_occurrences[seg_name] += 1

            # Count presence once per message
            if seg_name not in unique_segments_this_message:
                segments_presence[seg_name]["present_in"] += 1
                unique_segments_this_message.add(seg_name)

            # Mark segment as repeating if seen multiple times in the same message
            if segment_occurrences[seg_name] > 1:
                segments_presence[seg_name]["repeats"] = True

            if not segment_order or segment_order[-1] != seg_name:
                segment_order.append(seg_name)

        canonical_sequences.append(tuple(segment_order))

    # Count canonical sequences
    for seq in canonical_sequences:
        sequence_counts[seq] += 1

    # Create base annotated sequences (with repeat markers)
    annotated_sequences = {}
    for seq, count in sequence_counts.items():
        annotated_seq = []
        for seg in seq:
            if segments_presence[seg]["repeats"]:
                annotated_seq.append(f"{seg}+")
            else:
                annotated_seq.append(seg)
        annotated_sequences[tuple(annotated_seq)] = count

    # Merge similar patterns
    merged_patterns = merge_similar_patterns(annotated_sequences, segments_presence, total_messages)

    # Create final sequence list with optional markers
    final_sequences = []
    for pattern, count in merged_patterns.items():
        # Apply final optional marking
        final_pattern = []
        for seg in pattern:
            base_seg = clean_segment_name(seg)
            is_repeat = seg.endswith('+')
            is_optional = segments_presence[base_seg]["present_in"] < total_messages
            
            if is_optional and is_repeat:
                final_pattern.append(f"[{base_seg}+]")
            elif is_optional:
                final_pattern.append(f"[{base_seg}]")
            else:
                final_pattern.append(seg)
        
        final_sequences.append({
            "sequence": final_pattern,
            "count": count,
            "percent": round(count / total_messages * 100, 1)
        })

    # Sort by count (most common first)
    final_sequences.sort(key=lambda x: x["count"], reverse=True)

    return {
        "common_sequences": final_sequences,
        "segments": dict(segments_presence),
        "total_messages": total_messages
    }

def merge_similar_patterns(patterns, segments_presence, total_messages):
    """Merge similar patterns by identifying optional segments"""
    merged = {}
    pattern_list = list(patterns.items())
    processed = set()
    
    for i, (pattern1, count1) in enumerate(pattern_list):
        if pattern1 in processed:
            continue
            
        # Start with this pattern
        merged_pattern = list(pattern1)
        merged_count = count1
        processed.add(pattern1)
        
        # Look for similar patterns to merge
        for j, (pattern2, count2) in enumerate(pattern_list[i+1:], i+1):
            if pattern2 in processed:
                continue
                
            # Check if patterns can be merged
            merge_result = try_merge_patterns(list(pattern1), list(pattern2), segments_presence, total_messages)
            if merge_result:
                merged_pattern = merge_result
                merged_count += count2
                processed.add(pattern2)
        
        merged[tuple(merged_pattern)] = merged_count
    
    return merged

def try_merge_patterns(pattern1, pattern2, segments_presence, total_messages):
    """Try to merge two similar patterns by identifying optional segments"""
    # If patterns are identical, no merge needed
    if pattern1 == pattern2:
        return None
    
    # Simple case: one pattern is a subset of another (missing segments should be optional)
    if is_subsequence(pattern1, pattern2):
        return merge_with_optional_segments(pattern2, pattern1, segments_presence, total_messages)
    elif is_subsequence(pattern2, pattern1):
        return merge_with_optional_segments(pattern1, pattern2, segments_presence, total_messages)
    
    # More complex case: patterns have different optional segments
    merged = find_common_structure(pattern1, pattern2, segments_presence, total_messages)
    return merged if merged != pattern1 and merged != pattern2 else None

def is_subsequence(shorter, longer):
    """Check if shorter pattern is a subsequence of longer pattern"""
    if len(shorter) >= len(longer):
        return False
    
    i = 0
    for seg in longer:
        if i < len(shorter) and clean_segment_name(shorter[i]) == clean_segment_name(seg):
            i += 1
    
    return i == len(shorter)

def merge_with_optional_segments(longer_pattern, shorter_pattern, segments_presence, total_messages):
    """Merge patterns where one is a subset of another"""
    merged = []
    shorter_segments = [clean_segment_name(s) for s in shorter_pattern]
    
    for seg in longer_pattern:
        clean_seg = clean_segment_name(seg)
        if clean_seg in shorter_segments:
            # Segment appears in both patterns
            merged.append(seg)
        else:
            # Segment only in longer pattern - should be optional if presence < total
            if segments_presence[clean_seg]["present_in"] < total_messages:
                if seg.endswith('+'):
                    merged.append(f"[{clean_seg}+]")
                else:
                    merged.append(f"[{clean_seg}]")
            else:
                merged.append(seg)
    
    return merged

def find_common_structure(pattern1, pattern2, segments_presence, total_messages):
    """Find common structure between two patterns"""
    # Create a merged pattern that includes all segments from both patterns
    # marking segments that don't appear in both as optional
    
    p1_segments = [clean_segment_name(s) for s in pattern1]
    p2_segments = [clean_segment_name(s) for s in pattern2]
    
    # Find all unique segments while preserving order
    all_segments = []
    seen = set()
    
    # First pass: add segments from pattern1
    for seg in pattern1:
        clean_seg = clean_segment_name(seg)
        if clean_seg not in seen:
            all_segments.append(seg)
            seen.add(clean_seg)
    
    # Second pass: add missing segments from pattern2 in their relative positions
    p2_index = 0
    for i, seg in enumerate(pattern2):
        clean_seg = clean_segment_name(seg)
        if clean_seg not in seen:
            # Find where to insert this segment
            insert_pos = find_insertion_point(all_segments, seg, pattern2, p2_index)
            all_segments.insert(insert_pos, seg)
            seen.add(clean_seg)
        p2_index += 1
    
    # Mark segments as optional if they don't appear in both patterns
    merged = []
    for seg in all_segments:
        clean_seg = clean_segment_name(seg)
        in_p1 = clean_seg in p1_segments
        in_p2 = clean_seg in p2_segments
        
        if in_p1 and in_p2:
            # Appears in both - keep as is
            merged.append(seg)
        else:
            # Only appears in one - make optional if presence indicates it
            if segments_presence[clean_seg]["present_in"] < total_messages:
                if seg.endswith('+'):
                    merged.append(f"[{clean_seg}+]")
                else:
                    merged.append(f"[{clean_seg}]")
            else:
                merged.append(seg)
    
    return merged

def find_insertion_point(existing_segments, new_segment, source_pattern, source_index):
    """Find the best position to insert a new segment"""
    # Simple heuristic: insert at the end for now
    # This could be improved with more sophisticated positioning logic
    return len(existing_segments)

def clean_segment_name(segment):
    """Extract the base segment name from annotated versions"""
    segment = str(segment)
    # Remove brackets, plus signs, etc.
    return segment.replace('[', '').replace(']', '').replace('+', '')

def profile_sequences(messages):
    """Legacy function for backward compatibility - profiles all messages as one group"""
    return profile_sequences_for_type(messages)