"""
Reporter - Main orchestrator for generating HL7 analysis reports (Performance Optimized)
"""
import json
import logging
from pathlib import Path
import time
from datetime import datetime

logger = logging.getLogger(__name__)

def write_json_spec(aggregated, output_file, combined_only=False):
    """Main function to generate only the JSON spec from aggregated HL7 data"""
    logger.info("Starting report generation...")
    start_time = time.time()
    spec = aggregated["spec"]
    repeats = aggregated["repeats"]
    presence = aggregated["presence"]
    totals_by_type = aggregated.get("totals_by_type", {})
    sequence_profiles_by_type = aggregated.get("sequence_profiles_by_type", {})
    output_dir = Path(output_file).parent

    # Step 1: Generate combined spec (fast)
    from spec_builder import create_combined_spec
    logger.info("Creating combined specification...")
    combined_start = time.time()
    combined_spec = create_combined_spec(spec, presence, repeats, totals_by_type)
    combined_time = time.time() - combined_start
    logger.info(f"Combined spec created in {combined_time:.2f}s")

    # Step 2: Write combined JSON with sequence data (very fast)
    logger.info("Writing combined JSON...")
    json_start = time.time()
    combined_spec_with_metadata = {
        **combined_spec,
        "_sequence_profiles_by_type": sequence_profiles_by_type,
        "_totals_by_type": totals_by_type,
        "_metadata": {
            "total_messages": sum(totals_by_type.values()),
            "message_types": list(totals_by_type.keys()),
            "generated_at": datetime.now().isoformat(),
            "report_type": "combined"
        }
    }
    Path(output_file).write_text(json.dumps(combined_spec_with_metadata, indent=2))
    json_time = time.time() - json_start
    logger.info(f"Combined JSON written in {json_time:.2f}s")
    total_time = time.time() - start_time
    logger.info("=" * 50)
    logger.info("REPORT GENERATION SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Combined spec:      {combined_time:.2f}s")
    logger.info(f"Combined JSON:      {json_time:.2f}s")
    logger.info(f"Total time:         {total_time:.2f}s")
    logger.info(f"✅ Generated 1 JSON spec file")
    logger.info(f"📈 Report generation rate: {1/total_time:.1f} files/second")