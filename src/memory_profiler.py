"""
Memory profiler utility for monitoring HL7 processing performance
"""
import psutil
import os
import time
import logging
from functools import wraps

logger = logging.getLogger(__name__)

class MemoryMonitor:
    """Monitor memory usage during processing"""
    
    def __init__(self):
        self.process = psutil.Process(os.getpid())
        self.start_memory = 0
        self.peak_memory = 0
        self.measurements = []
    
    def start(self):
        """Start monitoring"""
        self.start_memory = self.get_memory_mb()
        self.peak_memory = self.start_memory
        logger.info(f"Starting memory monitoring - Initial: {self.start_memory:.1f} MB")
    
    def checkpoint(self, label=""):
        """Record a memory checkpoint"""
        current_memory = self.get_memory_mb()
        self.peak_memory = max(self.peak_memory, current_memory)
        
        self.measurements.append({
            'label': label,
            'memory_mb': current_memory,
            'delta_mb': current_memory - self.start_memory,
            'timestamp': time.time()
        })
        
        if label:
            logger.info(f"Memory checkpoint '{label}': {current_memory:.1f} MB (+{current_memory - self.start_memory:.1f} MB)")
    
    def get_memory_mb(self):
        """Get current memory usage in MB"""
        return self.process.memory_info().rss / 1024 / 1024
    
    def summary(self):
        """Print memory usage summary"""
        current_memory = self.get_memory_mb()
        logger.info("=" * 50)
        logger.info("MEMORY USAGE SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Initial memory:  {self.start_memory:.1f} MB")
        logger.info(f"Peak memory:     {self.peak_memory:.1f} MB")
        logger.info(f"Final memory:    {current_memory:.1f} MB")
        logger.info(f"Total increase:  {current_memory - self.start_memory:.1f} MB")
        logger.info(f"Peak increase:   {self.peak_memory - self.start_memory:.1f} MB")
        
        if self.measurements:
            logger.info("\nMemory checkpoints:")
            for measurement in self.measurements:
                logger.info(f"  {measurement['label']}: {measurement['memory_mb']:.1f} MB (+{measurement['delta_mb']:.1f} MB)")

def memory_profile(func):
    """Decorator to profile memory usage of a function"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        monitor = MemoryMonitor()
        monitor.start()
        
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        
        monitor.checkpoint(f"After {func.__name__}")
        
        logger.info(f"Function '{func.__name__}' completed in {end_time - start_time:.2f}s")
        monitor.summary()
        
        return result
    return wrapper

def estimate_memory_needs(file_count, total_size_mb):
    """Estimate memory requirements for processing"""
    # Rough estimates based on typical HL7 processing patterns
    base_memory = 50  # Base Python + libraries
    
    # File parsing: ~3x file size for parsing overhead
    parsing_memory = total_size_mb * 3
    
    # Aggregation: varies by message complexity, ~1-2x parsed size
    aggregation_memory = total_size_mb * 2
    
    # Report generation: usually small
    reporting_memory = 20
    
    estimated_peak = base_memory + parsing_memory + aggregation_memory + reporting_memory
    
    logger.info("=" * 50)
    logger.info("MEMORY ESTIMATION")
    logger.info("=" * 50)
    logger.info(f"Input files:           {file_count:,} files ({total_size_mb:.1f} MB)")
    logger.info(f"Base memory:           {base_memory:.1f} MB")
    logger.info(f"Parsing overhead:      {parsing_memory:.1f} MB")
    logger.info(f"Aggregation memory:    {aggregation_memory:.1f} MB")
    logger.info(f"Reporting memory:      {reporting_memory:.1f} MB")
    logger.info(f"Estimated peak:        {estimated_peak:.1f} MB")
    
    # Get available system memory
    available_memory = psutil.virtual_memory().available / 1024 / 1024
    logger.info(f"Available system RAM:  {available_memory:.1f} MB")
    
    if estimated_peak > available_memory * 0.8:  # Use 80% as safety margin
        logger.warning("⚠️  Estimated memory usage may exceed available RAM!")
        logger.warning("Consider using --streaming mode or processing in smaller batches")
        return False
    elif estimated_peak > available_memory * 0.5:
        logger.warning("Estimated memory usage is significant. Consider monitoring closely.")
        return True
    else:
        logger.info("✅ Memory usage should be within acceptable limits")
        return True

def get_optimal_workers(file_count, total_size_mb):
    """Calculate optimal number of worker processes"""
    cpu_cores = cpu_count()
    available_memory_gb = psutil.virtual_memory().available / 1024 / 1024 / 1024
    
    # Memory-based limit: assume each worker needs ~500MB peak
    memory_limit = max(1, int(available_memory_gb * 0.7 / 0.5))  # 70% of RAM, 500MB per worker
    
    # CPU-based limit
    cpu_limit = cpu_cores
    
    # File-based limit: don't create more workers than files
    file_limit = min(file_count, 8)  # Cap at 8 for diminishing returns
    
    # Size-based limit: for very large files, use fewer workers
    if total_size_mb > 1000:  # > 1GB
        size_limit = max(1, cpu_cores // 2)
    else:
        size_limit = cpu_cores
    
    optimal = min(memory_limit, cpu_limit, file_limit, size_limit)
    
    logger.info(f"Worker calculation: CPU={cpu_cores}, Memory_limit={memory_limit}, File_limit={file_limit}, Size_limit={size_limit}")
    logger.info(f"Optimal workers: {optimal}")
    
    return optimal