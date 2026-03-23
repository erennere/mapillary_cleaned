"""
Mapillary Metadata Download Pipeline

Orchestrates parallel downloading of Mapillary image metadata through an hierarchical
bounding box subdivision strategy. Uses dynamic rate limiting based on concurrent job
monitoring to maximize throughput while respecting API constraints.

Module Structure:
- Monitoring: Background threads tracking job state and rate limits
- API: Core HTTP communication and data retrieval
- Bbox Operations: Spatial subdivision of geographic areas
- Sequence Discovery: Iterative API querying with bbox refinement
- Metadata Download: Batch processing of image metadata
- Main: Orchestration and execution

Global State:
- Thread control: thread_stop, write_true
- Data buffers: global_sequences, global_bboxes, metadata_list, missing_sequences_list
- Rate limiting: allowed_connection, allowed_connection_current, number_of_jobs_running
"""

import glob
import math
import os
import time
import logging
import sys
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Tuple, Set, Optional, Callable, Any
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import box
from shapely import Point
import aiohttp
from tqdm import tqdm
from start import load_config

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

sys.setrecursionlimit(20000)


# ============================================================================
# CONSTANTS
# ============================================================================

API_MAX_RESULTS_PER_BBOX = 2000
FILE_AGE_THRESHOLD_SECONDS = 450
ZERO_DIVISION_SAFETY_FACTOR = 0.1


# ============================================================================
# GLOBAL STATE
# ============================================================================

thread_stop = False
write_true = False

number_of_jobs_running = 1
number_of_requests = 0
allowed_connection = 10000
allowed_connection_current = 10000

global_sequences: Set[str] = set()
global_bboxes: Set[Tuple[float, float, float, float]] = set()
metadata_list: List[pd.DataFrame] = []
missing_sequences_list: List[str] = []

# Thread synchronization locks - each protects specific global variables
metadata_lock = threading.Lock()  # Protects: metadata_list
missing_sequences_lock = threading.Lock()  # Protects: missing_sequences_list
lock = threading.Lock()  # Protects: global_sequences
lock_bbox = threading.Lock()  # Protects: global_bboxes
allowed_connection_lock = threading.Lock()  # Protects: allowed_connection, allowed_connection_current
request_count_lock = threading.Lock()  # Protects: number_of_requests


# ============================================================================
# SECTION: Monitoring - Background thread utilities
# ============================================================================

def check_timeout_function(start: bool, interval: float, check_timeout: float) -> int:
    """
    Sleep loop helper that checks for stop signals at regular intervals.
    
    Args:
        start (bool): If True, return immediately with code -1 for initial call.
        interval (float): Total sleep duration in seconds.
        check_timeout (float): Check frequency in seconds.
    
    Returns:
        int: -1 if start=True, 0 if thread_stop set, 1 if interval completed normally.
    """
    if start:
        logger.debug("Timeout check: start mode, returning early")
        return -1
    
    for _ in range(math.ceil(interval / (check_timeout + 0.1))):
        if thread_stop:
            logger.debug("Timeout check: thread_stop signal received")
            return 0
        time.sleep(check_timeout)
    
    logger.debug(f"Timeout check: completed {interval}s interval normally")
    return 1


def monitor_jobs(patterns: List[Dict[str, Any]], interval: int = 10, start: bool = False, check_timeout: int = 10, max_connections: int = 10000) -> None:
    """
    Monitor files matching glob patterns and adjust rate limits.
    
    Counts recently-modified files matching glob patterns to estimate concurrent
    job count. Dynamically adjusts allowed_connection to balance throughput across jobs.
    
    Args:
        patterns (list): List of pattern configurations, each a dict with:
            - 'pattern' (str): Glob file path pattern (e.g., 'path/to/file_*.csv')
            - 'threshold' (int): Max file age in seconds for this pattern
        interval (int): Monitoring cycle duration in seconds.
        start (bool): If True, perform single check then exit.
        check_timeout (int): Check interval within each cycle.
        max_connections (int): Maximum allowed connections globally.
    """
    global thread_stop, allowed_connection, number_of_jobs_running
    
    while not thread_stop:
        try:
            all_files = []
            for pattern_config in patterns:
                pattern = pattern_config.get('pattern')
                threshold = pattern_config.get('threshold', FILE_AGE_THRESHOLD_SECONDS)
                
                matching_files = [
                    file for file in glob.glob(pattern)
                    if os.path.isfile(file)
                    and (time.time() - os.path.getmtime(file)) < threshold
                ]
                all_files.extend(matching_files)
            
            jobs_count = len(all_files)

            with allowed_connection_lock:
                number_of_jobs_running = jobs_count

                if number_of_jobs_running > 0:
                    allowed_connection = round(max_connections / (number_of_jobs_running + ZERO_DIVISION_SAFETY_FACTOR))
                else:
                    allowed_connection = max_connections
            
            logger.debug(f"Jobs running: {number_of_jobs_running}, connections per job: {allowed_connection}")
        
        except (OSError, IOError, TypeError, ValueError) as e:
            logger.warning(f"Error in monitor_jobs: {e}")
        
        check_timeout_result = check_timeout_function(start, interval, check_timeout)
        if check_timeout_result == -1:
            break
        elif check_timeout_result == 0:
            return


def monitor_connections(interval: int = 60, start: bool = False, check_timeout: int = 10) -> None:
    """
    Periodically reset available connection quota for current job.
    
    Replenishes allowed_connection_current based on allowed_connection value
    at regular intervals to smoothly distribute rate limiting.
    
    Args:
        interval (int): Reset cycle duration in seconds.
        start (bool): If True, perform single reset then exit.
        check_timeout (int): Check interval within each cycle.
    """
    global thread_stop, allowed_connection_lock, allowed_connection, allowed_connection_current
    
    while not thread_stop:
        try:
            with allowed_connection_lock:
                allowed_connection_current = allowed_connection
                logger.debug(f"Connection reset: {allowed_connection_current} available")
        except Exception as e:
            logger.warning(f"Error in monitor_connections: {e}")
        
        check_timeout_result = check_timeout_function(start, interval, check_timeout)
        if check_timeout_result == -1:
            break
        elif check_timeout_result == 0:
            return


def write_sequences(sequence_file: str, interval: int = 300, start: bool = False, check_timeout: int = 10) -> None:
    """
    Periodically persist discovered sequences to CSV file.
    
    Background thread that writes global_sequences to disk at intervals,
    enabling progress tracking and resumability.
    
    Args:
        sequence_file (str): Output CSV file path.
        interval (int): Write cycle duration in seconds.
        start (bool): If True, write once then exit.
        check_timeout (int): Check interval within each cycle.
    """
    global thread_stop, lock, global_sequences
    
    while not thread_stop:
        try:
            with lock:
                sequences = pd.DataFrame({'sequences': list(global_sequences)})
                sequences.to_csv(sequence_file, index=False)
                logger.debug(f"Wrote {len(global_sequences)} sequences to {sequence_file}")
        except Exception as e:
            logger.error(f"Error writing sequences to {sequence_file}: {e}")
        
        check_timeout_result = check_timeout_function(start, interval, check_timeout)
        if check_timeout_result == -1:
            break
        elif check_timeout_result == 0:
            return


def write_bbox(bbox_file: str, interval: int = 300, start: bool = False, check_timeout: int = 10) -> None:
    """
    Periodically persist completed bounding boxes to GeoPackage file.
    
    Background thread that writes global_bboxes to disk at intervals,
    tracking which geographic areas have been queried.
    
    Args:
        bbox_file (str): Output GeoPackage file path.
        interval (int): Write cycle duration in seconds.
        start (bool): If True, write once then exit.
        check_timeout (int): Check interval within each cycle.
    """
    global thread_stop, lock_bbox, global_bboxes
    
    while not thread_stop:
        try:
            with lock_bbox:
                bboxes = create_geodataframe_from_bboxes(list(global_bboxes))
                if len(bboxes):
                    bboxes.to_file(bbox_file, driver='GPKG', index=False)
                    logger.debug(f"Wrote {len(bboxes)} bboxes to {bbox_file}")
        except Exception as e:
            logger.error(f"Error writing bboxes to {bbox_file}: {e}")
        
        check_timeout_result = check_timeout_function(start, interval, check_timeout)
        if check_timeout_result == -1:
            break
        elif check_timeout_result == 0:
            return


def write_data(df: pd.DataFrame, filepath: str) -> None:
    """
    Write DataFrame to CSV, appending if file exists.
    
    Args:
        df (pd.DataFrame): Data to write.
        filepath (str): Output CSV path.
    """
    df.reset_index(drop=True, inplace=True)
    if os.path.isfile(filepath):
        df.to_csv(filepath, mode='a', header=False, index=False)
        logger.debug(f"Appended {len(df)} rows to {filepath}")
    else:
        df.to_csv(filepath, index=False)
        logger.debug(f"Created {filepath} with {len(df)} rows")


def flush_metadata_buffer(filepath: str) -> None:
    """
    Write buffered metadata to disk and clear buffer.
    
    Args:
        filepath (str): Output CSV path.
    """
    global metadata_lock, metadata_list
    
    try:
        with metadata_lock:
            if metadata_list:
                metadata_df = pd.concat(metadata_list)
                write_data(metadata_df, filepath)
                rows = len(metadata_list)
                metadata_list.clear()
                logger.info(f"Flushed {rows} metadata batches to {filepath}")
    except Exception as e:
        logger.error(f"Error flushing metadata buffer: {e}")


def flush_missing_sequences_buffer(filepath: str) -> None:
    """
    Write buffered missing sequences to disk and clear buffer.
    
    Args:
        filepath (str): Output CSV path.
    """
    global missing_sequences_lock, missing_sequences_list
    
    try:
        with missing_sequences_lock:
            if missing_sequences_list:
                missing_df = pd.DataFrame({'sequence': missing_sequences_list})
                write_data(missing_df, filepath)
                count = len(missing_sequences_list)
                missing_sequences_list.clear()
                logger.info(f"Flushed {count} missing sequences to {filepath}")
    except Exception as e:
        logger.error(f"Error flushing missing sequences buffer: {e}")


def write_data_on_the_fly(filepath: str, flush_func: Callable, end: bool = False, interval: int = 300, check_timeout: int = 10) -> None:
    """
    Periodically flush buffered data to disk in background thread.
    
    Args:
        filepath (str): Output file path.
        flush_func (callable): Buffer flushing function (flush_metadata_buffer or flush_missing_sequences_buffer).
        end (bool): If True, flush once then exit.
        interval (int): Flush cycle duration in seconds.
        check_timeout (int): Check interval within each cycle.
    """
    global write_true
    
    while write_true:
        try:
            flush_func(filepath)
        except Exception as e:
            logger.error(f"Error in write_data_on_the_fly: {e}")
        
        if end:
            logger.debug(f"Write-on-the-fly ending (end=True)")
            break
        
        for _ in range(math.ceil(interval / (check_timeout + 0.1))):
            if not write_true:
                try:
                    flush_func(filepath)
                except Exception as e:
                    logger.error(f"Error in final flush: {e}")
                return
            time.sleep(check_timeout)


# ============================================================================
# SECTION: API - HTTP communication and data retrieval
# ============================================================================

async def async_get_response(session: aiohttp.ClientSession, url: str) -> Optional[Dict[str, Any]]:
    """
    Asynchronously fetch JSON response from URL with error handling.
    
    Args:
        url (str): API endpoint URL.
    
    Returns:
        dict or None: Parsed JSON response, None on error.
    """
    global number_of_requests
    
    try:
        async with session.get(url) as response:
            with request_count_lock:
                number_of_requests += 1

            if response is None:
                logger.warning(f"No response from {url}")
                return None

            json_data = await response.json()

            if response.status != 200:
                logger.warning(f"HTTP {response.status} from {url}: {json_data}")
                return None

            if isinstance(json_data, dict) and json_data.get('error'):
                error = json_data.get('error', {})
                msg = error.get('message') if isinstance(error, dict) else str(error)
                logger.warning(f"API error from {url}: {msg}")
                return None

            if json_data is None:
                logger.warning(f"Null JSON response from {url}")
                return None

            return json_data

    except Exception as e:
        logger.warning(f"Session/request error for {url}: {e}")
        return None


async def data_handling(session: aiohttp.ClientSession, url: str, attempt: int, empty_data_attempts: int, is_data_empty: int, sleep_time: int = 5, max_connections: int = 10000) -> Tuple[Optional[pd.DataFrame], int, int, int, float]:
    """
    Fetch and parse data from API with rate limiting and retry logic.
    
    Respects global connection quota, retries on empty responses, handles
    malformed data gracefully.
    
    Args:
        url (str): API endpoint.
        attempt (int): Current attempt number.
        empty_data_attempts (int): Max empty responses before giving up.
        is_data_empty (int): Current empty response count.
        sleep_time (int): Retry delay in seconds.
        max_connections (int): Global connection limit.
    
    Returns:
        tuple: (data_df, attempt, empty_data_attempts, is_data_empty, delay)
               where data_df is pd.DataFrame or None
    """
    global allowed_connection_lock, allowed_connection_current, number_of_jobs_running
    
    json_data = None
    try:
        while True:
            condition = False
            with allowed_connection_lock:
                if allowed_connection_current:
                    allowed_connection_current -= 1
                    condition = True
            
            if condition:
                json_data = await async_get_response(session, url)
                break
            await asyncio.sleep(sleep_time)
    
    except (asyncio.TimeoutError, asyncio.CancelledError, OSError) as e:
        logger.error(f"Error in data_handling for {url}: {e}")
    
    with allowed_connection_lock:
        jobs_running_snapshot = number_of_jobs_running
        current_connections_snapshot = allowed_connection_current

    factor = jobs_running_snapshot if jobs_running_snapshot != 0 else 1
    factor *= current_connections_snapshot if current_connections_snapshot != 0 else 1
    delay_between_requests = round(60 / (max_connections * factor + ZERO_DIVISION_SAFETY_FACTOR), 5)
    
    if json_data is None:
        attempt += 1
        logger.debug(f"No data, attempt {attempt}, delaying {delay_between_requests}s")
        await asyncio.sleep(delay_between_requests)
        return None, attempt, empty_data_attempts, is_data_empty, delay_between_requests
    
    data = json_data.get('data', None)
    if data is None:
        logger.warning(f"Unexpected JSON format from {url}")
        attempt += 1
        await asyncio.sleep(delay_between_requests)
        return None, attempt, empty_data_attempts, is_data_empty, delay_between_requests
    
    data = pd.DataFrame(data)
    if data.empty:
        if is_data_empty >= empty_data_attempts:
            logger.debug(f"Empty data limit reached ({is_data_empty}/{empty_data_attempts})")
            return None, attempt, empty_data_attempts, is_data_empty, delay_between_requests
        
        is_data_empty += 1
        logger.debug(f"Empty data response {is_data_empty}/{empty_data_attempts}")
        await asyncio.sleep(delay_between_requests)
        return None, attempt, empty_data_attempts, is_data_empty, delay_between_requests
    
    logger.debug(f"Retrieved {len(data)} records from {url}")
    return data, attempt, empty_data_attempts, is_data_empty, delay_between_requests


async def fetch_sequence_paginated_images(sequence: str, url: str, session: aiohttp.ClientSession, call_limit: int = 5, empty_data_attempts: int = 3,
                                      retries: int = 5, sleep_time: int = 5, max_connections: int = 10000) -> Tuple[pd.DataFrame, Optional[str]]:
    """
    Fetches image records for sequence, iteratively requesting until no new images found.
    Handles paginated API responses with de-duplication and early stopping.
    
    Args:
        sequence (str): Sequence ID.
        url (str): API endpoint.
        call_limit (int): Max API calls per sequence.
        empty_data_attempts (int): Max empty responses before abort.
        retries (int): Max consecutive calls without new images.
        sleep_time (int): Connection retry delay.
        max_connections (int): Global rate limit.
    
    Returns:
        tuple: (images_df, missing_sequence_id)
               images_df is pd.DataFrame with all images, missing_sequence_id is sequence or None
    """
    metadata_images = []
    seen_ids = set()
    attempt = 0
    is_data_empty = 0
    tracker = 0
    is_first_batch = True
    
    while attempt < call_limit:
        data, attempt, empty_data_attempts, is_data_empty, delay = await data_handling(
            session, url, attempt, empty_data_attempts, is_data_empty, sleep_time, max_connections
        )
        
        if data is None:
            continue
        
        mask = data.apply(
            lambda x: all(k in x for k in ["id", "thumb_original_url", "computed_geometry", "captured_at"]),
            axis=1
        )
        df_filtered = data[mask]
        df_filtered = df_filtered[~df_filtered['id'].isin(seen_ids)]
        
        if not df_filtered.empty:
            metadata_images.append(df_filtered)
            seen_ids.update(df_filtered['id'].tolist())
            tracker = 0
            logger.debug(f"Sequence {sequence}: found {len(df_filtered)} new images")
        else:
            tracker += 1
        
        if tracker >= retries:
            logger.debug(f"Sequence {sequence}: no new images for {retries} calls, stopping")
            break
        
        if is_first_batch and len(data) < API_MAX_RESULTS_PER_BBOX:
            logger.debug(f"Sequence {sequence}: <{API_MAX_RESULTS_PER_BBOX} images, stopping early")
            break
        
        is_first_batch = False
    
    if metadata_images:
        result = pd.concat(metadata_images, ignore_index=True)
        logger.info(f"Sequence {sequence}: {len(result)} total images")
        return result, None
    else:
        logger.warning(f"Sequence {sequence}: no images retrieved")
        return pd.DataFrame(), sequence


async def process_one_sequence(session: aiohttp.ClientSession, sequence: str, mly_key: str, columns: List[str], args_dict: Dict[str, Any]) -> Tuple[pd.DataFrame, Optional[str]]:
    """
    Download metadata for single sequence.
    
    Fetches image records, constructs geometry, fills missing columns,
    returns standardized DataFrame.
    
    Args:
        sequence (str): Sequence ID.
        mly_key (str): Mapillary API key.
        columns (list): Expected output columns.
        args_dict (dict): API request parameters.
    
    Returns:
        tuple: (metadata_df, missing_sequence_id)
               metadata_df is pd.DataFrame, missing_sequence_id is sequence or None
    """
    url = f"https://graph.mapillary.com/images?access_token={mly_key}&sequence_ids={str(sequence)}&fields={','.join([x for x in columns if x != 'sequence'])}"
    
    metadata_images, sequence_info = await fetch_sequence_paginated_images(sequence, url, session, **args_dict)
    
    if metadata_images.empty:
        logger.debug(f"Sequence {sequence}: no metadata, returning empty")
        return pd.DataFrame(columns=columns), sequence
    
    df_data = pd.DataFrame(metadata_images)
    df_data['sequence'] = sequence
    df_data['geometry'] = df_data['computed_geometry'].apply(
        lambda x: Point(x.get('coordinates')) if isinstance(x, dict) else np.nan
    )
    df_data['long'] = df_data['computed_geometry'].apply(
        lambda x: x.get('coordinates')[0] if isinstance(x, dict) else np.nan
    )
    df_data['lat'] = df_data['computed_geometry'].apply(
        lambda x: x.get('coordinates')[1] if isinstance(x, dict) else np.nan
    )
    
    for col in columns:
        if col not in df_data.columns:
            df_data[col] = False if col == "is_pano" else np.nan
    
    if 'computed_geometry' in df_data.columns:
        df_data.drop(['computed_geometry'], inplace=True, axis=1)
    
    df_data.rename({
        'thumb_original_url': 'url',
        'captured_at': 'timestamp'
    }, axis=1, inplace=True)
    
    df_data = df_data[['sequence', 'id', 'url', 'long', 'lat', 'geometry',
                       'height', 'width', 'altitude', 'make', 'model',
                       'creator', 'is_pano', 'timestamp']]
    
    logger.info(f"Processed sequence {sequence}: {len(df_data)} images")
    return df_data, sequence_info


# ============================================================================
# SECTION: Bbox Operations - Spatial subdivision utilities
# ============================================================================

def segmented_bboxes(boundary_box: List[float], n: int) -> List[List[float]]:
    """
    Subdivide bounding box into n x n grid of smaller boxes.
    
    Args:
        boundary_box (list): [west, south, east, north]
        n (int): Target division count (subdivides into ceil(sqrt(n)) x ceil(sqrt(n)))
    
    Returns:
        list: List of [west, south, east, north] bboxes
    """
    west, south, east, north = boundary_box
    num_rows_cols = math.ceil(math.sqrt(n))
    boxes = []
    
    for i in range(num_rows_cols):
        for j in range(num_rows_cols):
            sub_box_west = round(west + i * (east - west) / num_rows_cols, 5)
            sub_box_east = round(west + (i + 1) * (east - west) / num_rows_cols, 5)
            sub_box_south = round(south + j * (north - south) / num_rows_cols, 5)
            sub_box_north = round(south + (j + 1) * (north - south) / num_rows_cols, 5)
            boxes.append([sub_box_west, sub_box_south, sub_box_east, sub_box_north])
    
    logger.debug(f"Subdivided bbox into {len(boxes)} boxes ({num_rows_cols}x{num_rows_cols})")
    return boxes


def create_geodataframe_from_bboxes(bboxes: List[Tuple[float, float, float, float]]) -> gpd.GeoDataFrame:
    """
    Convert bounding boxes to GeoDataFrame.
    
    Args:
        bboxes (list): List of [west, south, east, north] tuples/lists
    
    Returns:
        gpd.GeoDataFrame: GeoDataFrame with polygon geometry
    """
    polygons = [box(west, south, east, north) for west, south, east, north in bboxes]
    return gpd.GeoDataFrame({'geometry': polygons})


# ============================================================================
# SECTION: Sequence Discovery - Iterative bbox querying
# ============================================================================

async def generator_get_bboxes_and_sequences(session: aiohttp.ClientSession, bbox: List[float], n: int, mly_key: str, call_limit: int = 5, empty_data_attempts: int = 3,
                                             sleep_time: int = 5, max_connections: int = 10000):
    """
    Recursively query sequences in bbox, subdividing if max results (2000) reached.
    
    Yields to enable iteration counting. Updates global_sequences and global_bboxes
    sets as data is discovered.
    
    Args:
        bbox (list): [west, south, east, north]
        n (int): Target subdivisions for recursive splitting
        mly_key (str): Mapillary API key
        call_limit (int): Max API calls per bbox
        empty_data_attempts (int): Max empty responses
        sleep_time (int): Retry delay
        max_connections (int): Global rate limit
    
    Yields:
        None: One per completed bbox (for iteration counting)
    """
    global lock, global_sequences, lock_bbox, global_bboxes
    
    bbox_str = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"
    url = f"https://graph.mapillary.com/images?access_token={mly_key}&fields=sequence&bbox={bbox_str}"
    
    data = None
    attempt = 0
    is_data_empty = 0
    
    while attempt < call_limit:
        data, attempt, empty_data_attempts, is_data_empty, delay = await data_handling(
            session, url, attempt, empty_data_attempts, is_data_empty, sleep_time, max_connections
        )
        
        if data is None:
            continue
        
        if 'sequence' not in data.columns:
            attempt += 1
            logger.warning(f"'sequence' column missing in bbox {bbox_str}")
            await asyncio.sleep(delay)
        else:
            break
    
    if data is None or data.empty or 'sequence' not in data.columns:
        logger.debug(f"No valid sequences in bbox {bbox_str}")
        yield None
        return
    
    sequence_set = set(data['sequence'].unique())
    if sequence_set:
        with lock:
            global_sequences.update(sequence_set)
        logger.info(f"Bbox {bbox_str}: found {len(sequence_set)} sequences (total: {len(global_sequences)})")
    
    if len(data) == API_MAX_RESULTS_PER_BBOX:
        logger.debug(f"Bbox {bbox_str}: 2000 results, subdividing into {n}x{n}")
        sub_bboxes = segmented_bboxes(bbox, n)
        for sub_bbox in sub_bboxes:
            async for _ in generator_get_bboxes_and_sequences(
                session, sub_bbox, n, mly_key, call_limit, empty_data_attempts, sleep_time, max_connections
            ):
                yield None
    else:
        with lock_bbox:
            global_bboxes.add(tuple(bbox))
        logger.info(f"Bbox {bbox_str}: completed with {len(data)} results")
        yield None


async def get_bboxes_and_sequences(bbox: List[float], n: int, mly_key: str, args: Dict[str, Any]) -> None:
    """
    Entry point for recursive sequence/bbox discovery.
    
    Args:
        bbox (list): [west, south, east, north]
        n (int): Target subdivisions
        mly_key (str): Mapillary API key
        args (dict): Additional API parameters
    """
    async with aiohttp.ClientSession() as session:
        async for _ in generator_get_bboxes_and_sequences(session, bbox, n, mly_key, **args):
            pass


def get_bboxes_and_sequences_wrapping(bbox: List[float], n: int, mly_key: str, args: Dict[str, Any], windows: bool = False) -> None:
    """
    Synchronous wrapper for async sequence discovery (for threading).
    
    Args:
        bbox (list): [west, south, east, north]
        n (int): Target subdivisions
        mly_key (str): Mapillary API key
        args (dict): Additional API parameters
        windows (bool): If True, apply Windows event loop policy
    """
    if windows:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(get_bboxes_and_sequences(bbox, n, mly_key, args))


def get_sequences(bbox: List[float], mly_key: str, n: int, output_dir: str, number_of_initial_bboxes: int,
                  params: Dict[str, Any], job_patterns: List[Dict[str, Any]], job_id: int = 0, max_workers: int = 4, windows: bool = False, monitoring: Optional[Dict[str, int]] = None) -> Tuple[List, List]:
    """
    Orchestrate parallel discovery of sequences and completed bboxes.
    
    Spawns ThreadPoolExecutor to parallelize bbox querying across initial
    subdivisions. Manages monitoring threads for rate limiting and progress
    persistence.
    
    Args:
        bbox (list): [west, south, east, north] initial bbox
        mly_key (str): Mapillary API key
        n (int): Subdivisions for recursive splitting
        output_dir (str): Output directory
        number_of_initial_bboxes (int): Initial subdivision count
        params (dict): API request parameters
        job_patterns (list): File patterns for monitoring
        job_id (int): Job identifier for output filename
        max_workers (int): ThreadPoolExecutor worker count
        windows (bool): Apply Windows event loop policy
        monitoring (dict): Monitoring configuration with keys: monitor_interval,
                          monitor_check_timeout, write_interval, write_check_timeout
    
    Returns:
        tuple: (list of bboxes, list of sequences)
    """
    if monitoring is None:
        monitoring = {
            'monitor_interval': 10,
            'monitor_check_timeout': 10,
            'write_interval': 300,
            'write_check_timeout': 10
        }
    
    global thread_stop, allowed_connection_current, allowed_connection, global_sequences, global_bboxes, number_of_requests
    
    os.makedirs(output_dir, exist_ok=True)
    
    sequence_file = os.path.join(output_dir, f'sequences_{job_id}.csv')
    finished_bboxes = os.path.join(output_dir, f'finished_bboxes_{job_id}.gpkg')
    
    for f in [sequence_file, finished_bboxes]:
        if os.path.exists(f):
            os.remove(f)
    
    with lock:
        global_sequences.clear()
    with lock_bbox:
        global_bboxes.clear()

    thread_stop = False
    allowed_connection_current = allowed_connection
    
    write_sequences(sequence_file, interval=monitoring['write_interval'], start=True, check_timeout=monitoring['write_check_timeout'])
    write_bbox(finished_bboxes, interval=monitoring['write_interval'], start=True, check_timeout=monitoring['write_check_timeout'])
    
    monitor_jobs(job_patterns, interval=monitoring['monitor_interval'], start=True, check_timeout=monitoring['monitor_check_timeout'], max_connections=params['max_connections'])
    monitor_connections(interval=monitoring['monitor_interval'], start=True, check_timeout=monitoring['monitor_check_timeout'])
    
    logger.info(f"Starting sequence discovery - initial {number_of_initial_bboxes} bboxes")
    
    sequences_thread = threading.Thread(
        target=write_sequences, 
        args=(sequence_file, monitoring['write_interval'], False, monitoring['write_check_timeout']), 
        daemon=True
    )
    sequences_thread.start()
    
    bboxes_thread = threading.Thread(
        target=write_bbox, 
        args=(finished_bboxes, monitoring['write_interval'], False, monitoring['write_check_timeout']), 
        daemon=True
    )
    bboxes_thread.start()
    
    connections_thread = threading.Thread(
        target=monitor_connections, 
        args=(monitoring['monitor_interval'], False, monitoring['monitor_check_timeout']), 
        daemon=True
    )
    connections_thread.start()
    
    job_thread = threading.Thread(
        target=monitor_jobs, 
        args=(job_patterns, monitoring['monitor_interval'], False, monitoring['monitor_check_timeout'], params['max_connections']), 
        daemon=True
    )
    job_thread.start()
    
    bboxes = segmented_bboxes(bbox, number_of_initial_bboxes)
    number_of_requests_initial = number_of_requests
    time_discovery_start = time.time()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        list(executor.map(
            get_bboxes_and_sequences_wrapping,
            bboxes,
            [n] * len(bboxes),
            [mly_key] * len(bboxes),
            [params] * len(bboxes),
            [windows] * len(bboxes)
        ))
    
    thread_stop = True
    time_discovery_end = time.time()
    
    number_of_requests_final = number_of_requests
    requests_for_query = number_of_requests_final - number_of_requests_initial
    
    if requests_for_query > 0:
        avg_time_per_request = (time_discovery_end - time_discovery_start) / requests_for_query
    else:
        avg_time_per_request = 0
    
    logger.info(f"Sequence discovery completed: {len(global_sequences)} sequences in {round(time_discovery_end-time_discovery_start, 2)}s")
    logger.info(f"Requests: {requests_for_query} in {round(time_discovery_end-time_discovery_start, 2)}s (avg {round(avg_time_per_request, 2)}s/req)")
    
    global_sequences_df = pd.DataFrame({'sequences': list(global_sequences)})
    global_sequences_df.to_csv(sequence_file, index=False)
    
    bboxes_gdf = create_geodataframe_from_bboxes(list(global_bboxes))
    if len(bboxes_gdf):
        bboxes_gdf.to_file(finished_bboxes, driver='GPKG', index=False)
    
    sequences_thread.join(timeout=30)
    bboxes_thread.join(timeout=30)
    connections_thread.join(timeout=30)
    job_thread.join(timeout=30)
    
    logger.info("Sequence discovery threads stopped")
    return list(global_bboxes), list(global_sequences)


# ============================================================================
# SECTION: Metadata Download - Batch image metadata retrieval
# ============================================================================

def process_generator(sequences: List[str], batch_size: int = 100):
    """
    Generate batches of sequences for async processing.
    
    Args:
        sequences (list): Sequence IDs
        batch_size (int): Batch size
    
    Yields:
        list: Batch of sequence IDs
    """
    batch = []
    for seq in sequences:
        batch.append(seq)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


async def metadata_download(mly_key: str, sequences: List[str], columns: List[str], args_dict: Dict[str, Any], batch_size: int = 100) -> None:
    """
    Download metadata for sequences in batches with async concurrency.
    
    Args:
        mly_key (str): Mapillary API key
        sequences (list): Sequence IDs
        columns (list): Expected output columns
        args_dict (dict): API parameters
        batch_size (int): Concurrent tasks per batch
    """
    global metadata_lock, metadata_list, missing_sequences_lock, missing_sequences_list
    
    for batch in tqdm(process_generator(sequences, batch_size), desc="Batches"):
        async with aiohttp.ClientSession() as session:
            tasks = [
                process_one_sequence(session, seq, mly_key, columns, args_dict)
                for seq in batch if seq is not None
            ]
            results = await asyncio.gather(*tasks)
            results = [r for r in results if r is not None]

            for metadata_df, missing_seq in results:
                if metadata_df is not None and not metadata_df.empty:
                    with metadata_lock:
                        metadata_list.append(metadata_df)

                if missing_seq is not None:
                    with missing_sequences_lock:
                        missing_sequences_list.append(missing_seq)


def metadata_download_wrapping(mly_key: str, sequences: List[str], columns: List[str], args_dict: Dict[str, Any], batch_size: int = 100, windows: bool = False) -> None:
    """
    Synchronous wrapper for async metadata download (for threading).
    
    Args:
        mly_key (str): Mapillary API key
        sequences (list): Sequence IDs
        columns (list): Expected output columns
        args_dict (dict): API parameters
        batch_size (int): Batch size
        windows (bool): Apply Windows event loop policy
    """
    if windows:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(metadata_download(mly_key, sequences, columns, args_dict, batch_size))


def get_metadata(sequence_list: List[str], missing_sequences_file: str, metadata_file: str, mly_key: str, columns: List[str], params: Dict[str, Any],
                 job_patterns: List[Dict[str, Any]], max_workers: int = 4, batch_size: int = 100, windows: bool = False, monitoring: Optional[Dict[str, int]] = None) -> None:
    """
    Orchestrate parallel metadata download for sequences.
    
    Distributes sequence list across workers, manages monitoring and buffering
    threads for efficient disk I/O.
    
    Args:
        sequence_list (list): Sequence IDs
        missing_sequences_file (str): Output path for unretrievable sequences
        metadata_file (str): Output path for metadata
        mly_key (str): Mapillary API key
        columns (list): Expected columns
        params (dict): API request parameters
        job_patterns (list): File patterns for monitoring
        max_workers (int): ThreadPoolExecutor worker count
        batch_size (int): Async batch size
        windows (bool): Apply Windows event loop policy
        monitoring (dict): Monitoring configuration with keys: monitor_interval,
                          monitor_check_timeout, write_interval, write_check_timeout
    """
    if monitoring is None:
        monitoring = {
            'monitor_interval': 10,
            'monitor_check_timeout': 10,
            'write_interval': 300,
            'write_check_timeout': 10
        }
    
    global thread_stop, write_true, allowed_connection_current, allowed_connection, number_of_requests
    
    os.makedirs(os.path.dirname(metadata_file), exist_ok=True)

    with metadata_lock:
        metadata_list.clear()
    with missing_sequences_lock:
        missing_sequences_list.clear()
    
    thread_stop = False
    write_true = True
    allowed_connection_current = allowed_connection
    
    monitor_jobs(job_patterns, interval=monitoring['monitor_interval'], start=True, check_timeout=monitoring['monitor_check_timeout'], max_connections=params['max_connections'])
    monitor_connections(interval=monitoring['monitor_interval'], start=True, check_timeout=monitoring['monitor_check_timeout'])
    
    logger.info(f"Starting metadata download for {len(sequence_list)} sequences")
    
    job_thread = threading.Thread(
        target=monitor_jobs, 
        args=(job_patterns, monitoring['monitor_interval'], False, monitoring['monitor_check_timeout'], params['max_connections']),
        daemon=True
    )
    job_thread.start()
    
    connections_thread = threading.Thread(
        target=monitor_connections, 
        args=(monitoring['monitor_interval'], False, monitoring['monitor_check_timeout']),
        daemon=True
    )
    connections_thread.start()
    
    metadata_thread = threading.Thread(
        target=write_data_on_the_fly, 
        args=(metadata_file, flush_metadata_buffer, False, monitoring['write_interval'], monitoring['write_check_timeout']),
        daemon=True
    )
    metadata_thread.start()
    
    missing_thread = threading.Thread(
        target=write_data_on_the_fly, 
        args=(missing_sequences_file, flush_missing_sequences_buffer, False, monitoring['write_interval'], monitoring['write_check_timeout']),
        daemon=True
    )
    missing_thread.start()
    
    number_of_requests_initial = number_of_requests
    time_download_start = time.time()
    
    sequence_chunks = np.array_split(sequence_list, max_workers)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        list(executor.map(
            metadata_download_wrapping,
            [mly_key] * len(sequence_chunks),
            sequence_chunks,
            [columns] * len(sequence_chunks),
            [params] * len(sequence_chunks),
            [batch_size] * len(sequence_chunks),
            [windows] * len(sequence_chunks)
        ))
    
    thread_stop = True
    write_true = False
    time_download_end = time.time()
    
    number_of_requests_final = number_of_requests
    requests_for_download = number_of_requests_final - number_of_requests_initial
    
    if requests_for_download > 0:
        avg_time_per_request = (time_download_end - time_download_start) / requests_for_download
    else:
        avg_time_per_request = 0
    
    logger.info(f"Metadata download completed in {round(time_download_end-time_download_start, 2)}s")
    logger.info(f"Requests: {requests_for_download} in {round(time_download_end-time_download_start, 2)}s (avg {round(avg_time_per_request, 2)}s/req)")
    
    write_data_on_the_fly(metadata_file, flush_metadata_buffer, end=True, interval=monitoring['write_interval'], check_timeout=monitoring['write_check_timeout'])
    write_data_on_the_fly(missing_sequences_file, flush_missing_sequences_buffer, end=True, interval=monitoring['write_interval'], check_timeout=monitoring['write_check_timeout'])
    
    connections_thread.join(timeout=30)
    job_thread.join(timeout=30)
    missing_thread.join(timeout=30)
    metadata_thread.join(timeout=30)
    
    logger.info("Metadata download threads stopped")


# ============================================================================
# SECTION: Main - Orchestration
# ============================================================================

def main(bbox: List[float], mly_key: str, columns: List[str], n: int, output_dir: str, job_id: int, metadata_basename: str, missing_basename: str,
         initial_subdivisions: int, params: Dict[str, Any], max_workers: int = 4, enable_download: bool = True, batch_size: int = 100, 
         windows: bool = False, monitoring: Optional[Dict[str, int]] = None) -> None:
    """
    Orchestrate complete metadata download pipeline.
    
    Performs sequence discovery via recursive bbox subdivision, then
    downloads image metadata for discovered sequences. Configures monitoring
    patterns and passes them to orchestration functions.
    
    Args:
        bbox (list): [west, south, east, north] query area
        mly_key (str): Mapillary API key
        columns (list): Expected metadata columns
        n (int): Bbox subdivision factor
        output_dir (str): Output directory
        job_id (int): Job identifier
        metadata_basename (str): Output basename for metadata
        missing_basename (str): Output basename for missing sequences
        initial_subdivisions (int): Initial bbox subdivision count
        params (dict): API request parameters
        max_workers (int): Parallelization level
        enable_download (bool): If False, skip metadata download after sequence discovery
        batch_size (int): Async batch size
        windows (bool): Windows platform flag
        monitoring (dict): Monitoring configuration with keys: monitor_interval, 
                          monitor_check_timeout, write_interval, write_check_timeout
    """
    if monitoring is None:
        monitoring = {
            'monitor_interval': 10,
            'monitor_check_timeout': 10,
            'write_interval': 300,
            'write_check_timeout': 10
        }
    
    global number_of_requests
    with request_count_lock:
        number_of_requests = 0
    
    time_pipeline_start = time.time()
    
    metadata_file = os.path.join(output_dir, f'{metadata_basename}.csv')
    missing_file = os.path.join(output_dir, f'{missing_basename}.csv')
    
    for f in [metadata_file, missing_file]:
        if os.path.exists(f):
            os.remove(f)
    
    discovery_patterns = [
        {'pattern': os.path.join(output_dir, 'sequences_*.csv'), 'threshold': FILE_AGE_THRESHOLD_SECONDS},
        {'pattern': os.path.join(output_dir, 'metadata_unfiltered_*.csv'), 'threshold': FILE_AGE_THRESHOLD_SECONDS}
    ]
    
    metadata_dir = os.path.dirname(metadata_file)
    download_patterns = [
        {'pattern': os.path.join(metadata_dir, 'sequences_*.csv'), 'threshold': FILE_AGE_THRESHOLD_SECONDS},
        {'pattern': os.path.join(metadata_dir, 'metadata_unfiltered_*.csv'), 'threshold': FILE_AGE_THRESHOLD_SECONDS}
    ]
    
    logger.info(f"=== Pipeline Start: Job {job_id} ===")
    logger.info(f"Bbox: {bbox}, Workers: {max_workers}, Batch: {batch_size}")
    
    bboxes_list, sequence_list = get_sequences(
        bbox, mly_key, n, output_dir, initial_subdivisions,
        {k: v for k, v in params.items() if k != 'retries'},
        discovery_patterns, job_id, max_workers, windows, monitoring
    )
    
    logger.info(f"Discovered: {len(bboxes_list)} bboxes, {len(sequence_list)} sequences")
    
    if enable_download and len(sequence_list) > 0:
        get_metadata(
            sequence_list, missing_file, metadata_file,
            mly_key, columns, params, download_patterns, max_workers, batch_size, windows, monitoring
        )
    
    time_pipeline_end = time.time()
    total_time = round(time_pipeline_end - time_pipeline_start, 2)
    
    if number_of_requests > 0:
        avg_time_per_request = round(total_time / number_of_requests, 2)
    else:
        avg_time_per_request = 0
    
    logger.info(f"=== Pipeline Complete ===")
    logger.info(f"Total time: {total_time}s, Total requests: {number_of_requests}, Avg: {avg_time_per_request}s/req")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == '__main__':
    os.chdir(os.path.abspath(os.path.dirname(__file__)))
    config = load_config()
    
    job_id = str(sys.argv[1]) if len(sys.argv) > 1 else 0
    
    output_directory = os.path.abspath(config['paths']['raw_metadata_dir'])
    os.makedirs(output_directory, exist_ok=True)
    
    api_key = config['params']['mly_key']
    
    metadata_config = config['metadata_params']
    query_bbox = metadata_config['query_bbox']
    meta_basename = f"{metadata_config['metadata_basename']}_{job_id}"
    missing_basename = f"{metadata_config['missing_basename']}_{job_id}"
    initial_subdivisions = metadata_config['initial_subdivisions']
    subdivision_factor = metadata_config['subdivision_factor']
    enable_download = metadata_config['enable_download']
    max_workers = metadata_config.get('max_workers', 4)
    batch_size = metadata_config.get('batch_size', 100)
    windows_platform = metadata_config.get('windows', True)
    
    params = {
        'retries': metadata_config.get('retries', 5),
        'call_limit': metadata_config.get('call_limit', 5),
        'empty_data_attempts': metadata_config.get('empty_data_attempts', 3),
        'max_connections': metadata_config.get('max_connections', 10000),
        'sleep_time': metadata_config.get('sleep_time', 5)
    }
    
    monitoring = {
        'monitor_interval': metadata_config.get('monitor_interval', 10),
        'monitor_check_timeout': metadata_config.get('monitor_check_timeout', 10),
        'write_interval': metadata_config.get('write_interval', 300),
        'write_check_timeout': metadata_config.get('write_check_timeout', 10)
    }
    
    columns_list = config['metadata_columns']
    
    logger.info(f"Configuration loaded from config.yaml")
    logger.info(f"Job ID: {job_id}, API Key: {api_key[:20]}...")
    
    main(
        query_bbox, api_key, columns_list, subdivision_factor, output_directory, job_id,
        meta_basename, missing_basename, initial_subdivisions, params,
        max_workers, enable_download, batch_size, windows_platform, monitoring
    )
