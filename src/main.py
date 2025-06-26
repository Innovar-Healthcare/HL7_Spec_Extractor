import sys
import os
import json
import logging
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count
from multiprocessing import freeze_support
import time
from tqdm import tqdm
import base64
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend
from datetime import datetime

from hl7_parser import parse_hl7_message
from aggregator import aggregate_data
from reporter import write_json_spec
from sequence_profiler import profile_sequences


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hl7_analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def parse_single_file(file_path):
    """Parse a single HL7 file - designed for multiprocessing"""
    try:
        with open(file_path, "r", encoding='utf-8', errors='ignore') as f:
            content = f.read()
            parsed = parse_hl7_message(content)
            return parsed, None
    except Exception as e:
        return None, f"Error parsing {file_path}: {str(e)}"

def get_hl7_files(input_folder):
    """Get list of HL7 files with size information"""
    hl7_files = []
    total_size = 0
    
    for file_path in Path(input_folder).rglob("*.hl7"):
        size = file_path.stat().st_size
        hl7_files.append((str(file_path), size))
        total_size += size
    
    # Sort by size (smaller files first for better load balancing)
    hl7_files.sort(key=lambda x: x[1])
    
    logger.info(f"Found {len(hl7_files)} HL7 files ({total_size / 1024 / 1024:.1f} MB total)")
    return hl7_files

def parse_files_parallel(file_paths, max_workers=None):
    """Parse multiple HL7 files in parallel"""
    if max_workers is None:
        max_workers = min(cpu_count(), len(file_paths), 8)  # Cap at 8 to avoid overwhelming
    
    logger.info(f"Using {max_workers} parallel workers for parsing")
    
    messages = []
    errors = []
    
    start_time = time.time()
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all jobs
        future_to_file = {
            executor.submit(parse_single_file, file_path): file_path 
            for file_path, _ in file_paths
        }
        
        # Process results with progress bar
        with tqdm(total=len(file_paths), desc="Parsing HL7 files") as pbar:
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    parsed_msg, error = future.result()
                    if parsed_msg:
                        messages.append(parsed_msg)
                    if error:
                        errors.append(error)
                        logger.warning(error)
                except Exception as e:
                    error_msg = f"Failed to process {file_path}: {str(e)}"
                    errors.append(error_msg)
                    logger.error(error_msg)
                
                pbar.update(1)
    
    parse_time = time.time() - start_time
    logger.info(f"Parsed {len(messages)} messages in {parse_time:.2f}s ({len(messages)/parse_time:.1f} msg/sec)")
    
    if errors:
        logger.warning(f"Encountered {len(errors)} parsing errors")
    
    return messages, errors

def parse_files_streaming(file_paths, batch_size=1000):
    """Parse files in streaming batches for memory efficiency"""
    logger.info(f"Using streaming processing with batch size {batch_size}")
    
    all_messages = []
    current_batch = []
    errors = []
    
    with tqdm(total=len(file_paths), desc="Processing HL7 files") as pbar:
        for file_path, _ in file_paths:
            try:
                with open(file_path, "r", encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                    parsed = parse_hl7_message(content)
                    if parsed:
                        current_batch.append(parsed)
                
                # Process batch when it reaches target size
                if len(current_batch) >= batch_size:
                    all_messages.extend(current_batch)
                    current_batch = []
                    
            except Exception as e:
                error_msg = f"Error parsing {file_path}: {str(e)}"
                errors.append(error_msg)
                logger.warning(error_msg)
            
            pbar.update(1)
    
    # Process remaining messages
    if current_batch:
        all_messages.extend(current_batch)
    
    if errors:
        logger.warning(f"Encountered {len(errors)} parsing errors")
    
    return all_messages, errors

def validate_license(license_path, public_key_path):
    """Validate license signature and expiration."""
    import json
    try:
        with open(license_path, 'r') as f:
            license_data = json.load(f)
        signature = base64.b64decode(license_data['signature'])
        # Prepare data for verification (remove signature field)
        data_to_verify = dict(license_data)
        del data_to_verify['signature']
        # Canonical JSON encoding
        data_bytes = json.dumps(data_to_verify, sort_keys=True, separators=(",", ":")).encode('utf-8')
        # Load public key
        with open(public_key_path, 'rb') as f:
            public_key = serialization.load_pem_public_key(f.read(), backend=default_backend())
        # Verify signature
        public_key.verify(
            signature,
            data_bytes,
            padding.PKCS1v15(),
            hashes.SHA256()
        )
        # Check expiration
        expiration = datetime.fromisoformat(license_data['expiration'].replace('Z', '+00:00'))
        if expiration < datetime.now(expiration.tzinfo):
            logger.error('License expired!')
            return False
        return True
    except Exception as e:
        logger.error(f'License validation failed: {e}')
        return False

def main():
    if len(sys.argv) < 3:
        print("Usage: python main.py <input_folder> <output_file> [options]")
        print("Options:")
        print("  --parallel: Use parallel processing (default)")
        print("  --streaming: Use streaming processing for large datasets")
        print("  --workers N: Number of parallel workers (default: auto)")
        print("  --batch-size N: Batch size for streaming (default: 1000)")
        print("  --combined-only: Generate only combined reports (no individual message type reports)")
        sys.exit(1)

    input_folder = sys.argv[1]
    output_file = sys.argv[2]
    use_parallel = True
    max_workers = None
    batch_size = 1000
    combined_only = False
    i = 3
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == "--streaming":
            use_parallel = False
        elif arg == "--parallel":
            use_parallel = True
        elif arg == "--combined-only":
            combined_only = True
        elif arg == "--workers" and i + 1 < len(sys.argv):
            max_workers = int(sys.argv[i + 1])
            i += 1
        elif arg == "--batch-size" and i + 1 < len(sys.argv):
            batch_size = int(sys.argv[i + 1])
            i += 1
        i += 1

    if not os.path.exists(input_folder):
        logger.error(f"Input folder not found: {input_folder}")
        sys.exit(1)

    # Create output directory
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    start_time = time.time()
    
    # Get file list
    hl7_files = get_hl7_files(input_folder)
    if not hl7_files:
        logger.error("No HL7 files found in input folder")
        sys.exit(1)

    # Choose processing method based on dataset size and user preference
    total_size_mb = sum(size for _, size in hl7_files) / 1024 / 1024
    
    if use_parallel and total_size_mb < 1000:  # < 1GB, use parallel
        logger.info("Using parallel processing")
        messages, errors = parse_files_parallel(hl7_files, max_workers)
    else:  # Large dataset or streaming requested
        logger.info("Using streaming processing for large dataset")
        messages, errors = parse_files_streaming(hl7_files, batch_size)

    if not messages:
        logger.error("No valid HL7 messages found")
        sys.exit(1)

    # Log performance stats
    parse_time = time.time() - start_time
    logger.info(f"Total parsing time: {parse_time:.2f}s")
    logger.info(f"Processing rate: {len(messages)/parse_time:.1f} messages/second")

    # Aggregate data
    logger.info("Aggregating field statistics...")
    agg_start = time.time()
    aggregated = aggregate_data(messages)
    agg_time = time.time() - agg_start
    logger.info(f"Aggregation completed in {agg_time:.2f}s")

    # Profile sequences
    logger.info("Profiling segment sequences...")
    seq_start = time.time()
    sequence_profile = profile_sequences(messages)
    aggregated["segment_sequence_summary"] = sequence_profile
    seq_time = time.time() - seq_start
    logger.info(f"Sequence profiling completed in {seq_time:.2f}s")

    # Generate reports
    logger.info("Generating reports...")
    if combined_only:
        logger.info("🚀 Combined-only mode: Generating combined reports only")
    else:
        logger.info("🚀 Full mode: Generating combined + individual reports")
    
    report_start = time.time()
    write_json_spec(aggregated, output_file, combined_only=combined_only)
    report_time = time.time() - report_start
    logger.info(f"Report generation completed in {report_time:.2f}s")

    # Final stats
    total_time = time.time() - start_time
    logger.info(f"✅ Analysis complete!")
    logger.info(f"📊 Processed {len(messages)} messages")
    logger.info(f"⏱️  Total time: {total_time:.2f}s")
    logger.info(f"📁 Output written to {output_file}")
    
    if combined_only:
        logger.info(f"📋 Mode: Combined reports only (faster generation)")
    else:
        logger.info(f"📋 Mode: Full analysis with individual message type reports")
    
    if errors:
        logger.warning(f"⚠️  {len(errors)} files had parsing errors (see log for details)")

if __name__ == "__main__":
    freeze_support()
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")