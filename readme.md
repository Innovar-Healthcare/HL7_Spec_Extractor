# HL7 Specification Extractor

A high-performance Python tool for analyzing HL7 messages and automatically generating comprehensive field specifications. This tool processes HL7 v2.x messages from files to create detailed specifications that document field usage patterns, data types, value distributions, and segment sequences.

## 🎯 Purpose

Healthcare organizations often need to understand the structure and content of their HL7 message flows for:
- **Interface Documentation**: Creating comprehensive specs for system integrations
- **Data Mapping**: Understanding field usage patterns for system migrations
- **Compliance Analysis**: Documenting data flows for regulatory requirements
- **Quality Assurance**: Identifying data inconsistencies and missing fields
- **System Design**: Planning new integrations based on existing message patterns

This tool automatically analyzes thousands of HL7 messages and generates:
- **Detailed field specifications** with descriptions, data types, and usage statistics
- **Segment sequence analysis** showing common message patterns
- **Statistical summaries** of field presence and value distributions

## 🚀 Key Features

### Analysis Capabilities
- **Field-level analysis**: Data types, lengths, presence statistics, and value distributions
- **Segment sequence profiling**: Common patterns and variations across message types
- **Multi-message type support**: Separate specs for ADT, ORM, ORU, etc.
- **PHI-aware processing**: Automatically excludes sensitive fields from value analysis
- **Complex data type handling**: Proper parsing of XCN, CX, XAD, and other composite types

### Performance Optimizations
- **Parallel processing**: Multi-core parsing with configurable worker counts
- **Streaming mode**: Memory-efficient processing for large datasets
- **Progress tracking**: Resume interrupted processing sessions
- **Memory optimization**: Intelligent cleanup and batch processing

### Output Formats
- **JSON specifications**: Machine-readable field definitions and statistics
- **CSV exports**: Data for spreadsheet analysis

## 📋 Requirements

### System Requirements
- **Python**: 3.8 or higher
- **Memory**: 4GB+ RAM recommended for large datasets
- **Storage**: Temporary space for file processing (auto-cleaned)

### Python Dependencies
```bash
# Core dependencies
hl7                 # HL7 message parsing
tqdm                # Progress bars
collections         # Data structures (built-in)
pathlib            # Path handling (built-in)
multiprocessing    # Parallel processing (built-in)
```

## 🛠️ Installation

### 1. Create Virtual Environment
```bash
# Create and activate virtual environment
python3 -m venv hl7_env
source hl7_env/bin/activate  # Linux/macOS
# or
hl7_env\Scripts\activate     # Windows
```

### 2. Install Python Dependencies
```bash
# Core HL7 processing
pip install hl7 tqdm
```

### 3. Clone/Download Project
```bash
git clone <repository-url>
cd hl7-spec-extractor
```

## 📖 Usage

### Local File Processing

#### Basic Usage
```bash
# Process HL7 files and generate specifications
python main.py input_folder/ output_spec.json

# Example: Process sample HL7 files
python main.py ./hl7_samples/ ./results/hospital_spec.json
```

#### Advanced Options
```bash
# Use parallel processing (default)
python main.py input_folder/ output.json --parallel --workers 4

# Use streaming mode for large datasets (>1GB)
python main.py input_folder/ output.json --streaming --batch-size 500

# Generate only combined reports (faster)
python main.py input_folder/ output.json --combined-only

# Full example with all options
python main.py ./hl7_messages/ ./specs/full_analysis.json \
    --parallel \
    --workers 6 \
    --combined-only
```

#### Command Line Options
- `--parallel`: Use multi-core processing (default, recommended)
- `--streaming`: Use memory-efficient streaming mode for large datasets
- `--workers N`: Number of parallel workers (default: auto-detected)
- `--batch-size N`: Batch size for streaming mode (default: 1000)
- `--combined-only`: Generate only combined reports (skips individual message type reports)

## 📁 Project Structure

```
hl7-spec-extractor/
├── main.py                     # Main entry point for local file processing
├── hl7_parser.py              # HL7 message parsing and field extraction
├── aggregator.py              # Field statistics aggregation and analysis
├── spec_builder.py            # JSON specification generation
├── reporter.py                # Report orchestration
├── sequence_profiler.py       # Segment sequence analysis
├── memory_profiler.py         # Memory usage monitoring and optimization
├── hl7_fields.py              # HL7 field definitions and utilities
├── schemas/
│   ├── hl7_segments.json      # HL7 segment definitions
│   ├── hl7_fields.json        # HL7 field definitions
│   └── hl7_datatypes.json     # HL7 data type definitions
├── aws_credentials.json       # AWS credentials (optional, for legacy use)
├── prefixes.csv              # S3 prefix mapping (legacy, not used)
└── README.md                  # This file
```

### Core Modules

#### `main.py`
Main entry point for local file processing. Handles command-line arguments, file discovery, parallel/streaming processing coordination, and progress reporting.

#### `hl7_parser.py`
HL7 message parser that handles:
- Segment and field extraction
- Repetition detection
- Component and subcomponent parsing
- Message type identification
- Error handling for malformed messages

#### `aggregator.py`
Core aggregation engine that:
- Collects field statistics across messages
- Tracks presence patterns and repetitions
- Handles complex data types (XCN, PPN, CX, etc.)
- Optimizes memory usage with streaming processing
- Consolidates component fields intelligently

#### `spec_builder.py`
Specification builder that:
- Creates individual message type specifications
- Generates combined cross-message specifications
- Handles PHI field exclusion
- Calculates field statistics and distributions
- Manages unique value collection with limits

#### `reporter.py`
Report orchestration engine that:
- Coordinates JSON spec generation
- Handles performance optimization
- Provides progress tracking and error handling

#### `sequence_profiler.py`
Segment sequence analyzer that:
- Identifies common segment patterns
- Tracks segment presence and repetition
- Merges similar patterns with optional segments
- Profiles sequences by message type

#### Utilities

- **`memory_profiler.py`**: Memory usage monitoring and optimization
- **`hl7_fields.py`**: HL7 field definitions, descriptions, and utilities

## 📊 Output Files

When processing completes, you'll find these files in your output directory:

### For Individual Processing
- `spec.json` - Complete JSON specification
- `spec_[MessageType].json` - Individual message type specs (if implemented)

## 🔧 Configuration

### Performance Tuning

#### Memory Optimization
```bash
# For large datasets (>1GB), use streaming mode
python main.py input/ output.json --streaming --batch-size 500
```

#### Parallel Processing
```bash
# Adjust worker count based on CPU cores and memory
python main.py input/ output.json --workers 4

# For memory-constrained environments
python main.py input/ output.json --workers 2 --streaming
```

### PHI Field Exclusion

PHI fields are automatically excluded from value analysis but included in structural analysis. The system recognizes these patterns:
- Patient names (PID.5, PID.6, PID.9)
- Identifiers (PID.2, PID.3, PID.18, PID.19)
- Addresses (PID.11, GT1.5, NK1.4)
- Phone numbers (PID.13, PID.14, GT1.6, GT1.7)

## 📈 Performance Characteristics

### Processing Speeds
- **Local files**: ~1,000-5,000 messages/second (depending on complexity)
- **Memory usage**: ~2-4GB for 100,000 messages

### Optimization Features
- Automatic memory cleanup and garbage collection
- Streaming processing for unlimited dataset sizes
- Progress tracking and resume capability
- Intelligent field consolidation and deduplication

## 🛡️ Error Handling

The tool includes robust error handling:
- **Malformed HL7 messages**: Logged and skipped
- **Memory issues**: Automatic fallback to streaming mode
- **File access errors**: Detailed logging with suggested fixes
- **Process interruption**: Clean shutdown with progress saving

## 🔍 Troubleshooting

### Common Issues

#### Memory Issues
```bash
# Use streaming mode for large datasets
python main.py input/ output.json --streaming --batch-size 200

# Reduce worker count
python main.py input/ output.json --workers 2
```

#### No HL7 Files Found
```bash
# Check file extensions - tool looks for .hl7, .txt, .msg
# Verify directory structure and file permissions
```

### Debug Mode
```bash
# Enable detailed logging
export LOGLEVEL=DEBUG
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit issues, feature requests, or pull requests.

### Development Setup
```bash
# Clone and setup development environment
git clone <repository-url>
cd hl7-spec-extractor
python3 -m venv dev_env
source dev_env/bin/activate
pip install -r requirements.txt
```

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues, questions, or feature requests:
1. Check the troubleshooting section above
2. Review existing GitHub issues
3. Create a new issue with detailed information about your environment and the problem

## 🙏 Acknowledgments

- Built using the excellent `python-hl7` library for HL7 parsing
- Performance monitoring with `tqdm`