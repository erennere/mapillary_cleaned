#!/usr/bin/env python
# coding: utf-8
"""
Mapillary Image Downloader and Processor

Downloads images from Mapillary street-level imagery platform using provided URLs,
applies image processing (resizing), and saves both original and processed versions.
Supports parallel batch processing with rate limiting and error tracking.

This module can be used standalone via command-line or imported for programmatic use.
When imported, use the orchestrate() function with directory and metadata parameters.
"""
import random
import os, logging, time, math, sys
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import asyncio, aiohttp, threading
import cv2
import glob
from start import load_config

logging.basicConfig(level=logging.WARNING)


# ============================================================================
# Global State
# ============================================================================
lock = threading.Lock()
missing_images_lock = threading.Lock()
allowed_connection_lock = threading.Lock()
allowed_connections = 3000
allowed_connections_current = allowed_connections
missing_images = []
missing_image_ids = set()
thread_stop = True

# ============================================================================
# Background Monitoring Functions
# ============================================================================

def monitor_connections(interval=60, start=False, check_timeout=10):
    """
    Background thread that periodically replenishes the allowed connection counter.
    
    This allows the rate limiting system to gradually recover capacity over time,
    preventing indefinite stalling if the connection limit is fully exhausted.
    
    Args:
        interval (int): Time in seconds between replenishment cycles. Default: 60.
        start (bool): If True, exit immediately after first check. Default: False.
        check_timeout (int): Check interval within each cycle in seconds. Default: 10.
    """
    global thread_stop, lock, allowed_connections, allowed_connections_current
    while not thread_stop:
        with allowed_connection_lock:
            allowed_connections_current = allowed_connections
        if start: break
        for _ in range(math.ceil(interval/check_timeout)):
            if thread_stop: return
            time.sleep(check_timeout)

def write_missing_images(missing_images_file, interval=15, start=False, check_timeout=10):
    """
    Background thread that periodically writes failed image records to CSV.
    
    Collects images that failed to download and saves them to a CSV file
    at regular intervals, as well as on shutdown if there are pending records.
    
    Args:
        missing_images_file (str): Path to the CSV file for failed image records.
        interval (int): Time in seconds between write cycles. Default: 15.
        start (bool): If True, exit immediately after first check. Default: False.
        check_timeout (int): Check interval within each cycle in seconds. Default: 10.
    """
    global thread_stop, missing_images_lock, missing_images
    while not thread_stop:
        with missing_images_lock:
            images =  pd.DataFrame(missing_images)
            if len(images):
                images.to_csv(missing_images_file, index=False)
        if start: break
        for _ in range(math.ceil(interval/check_timeout)):
            if thread_stop:
                with missing_images_lock:
                    images = pd.DataFrame(missing_images)
                    if len(images):
                        images.to_csv(missing_images_file, index=False)
                return
            time.sleep(check_timeout)

# ============================================================================
# Image Fetching and Processing Functions
# ============================================================================

async def fetch_image(url, session):
    """
    Asynchronously fetches image data from a URL.
    
    Args:
        url (str): The URL from which to fetch the image.
        session (aiohttp.ClientSession): Reused HTTP session.
    
    Returns:
        bytes: Raw image data if successful, None on failure.
    """
    try:
        async with session.get(url) as response:
            if response.status != 200:
                logging.warning(f"Failed to fetch image from {url}. Status code: {response.status}")
                return None
            image_data = await response.read()
            if not isinstance(image_data, bytes):
                logging.warning(f"Invalid image data type received from {url}")
                return None
            if len(image_data):
                return image_data
            logging.warning(f"Empty image data received from {url}")
            return None
    except Exception as err:
        logging.warning(f"An exception occurred while fetching the image: {url}:{err}")
        return None
  
async def get_image(url, image_size, session, call_limit=5, sleep_time=5):
    """
    Fetches an image from a URL, applies rate limiting, and resizes it.
    
    Respects connection rate limiting by waiting for available capacity. Retries
    on failure with exponential backoff based on remaining connection quota.

    Args:
        url (str): The URL from which to fetch the image.
        image_size (tuple): (width, height) for resizing the image after fetching.
        session (aiohttp.ClientSession): Reused HTTP session.
        call_limit (int): Maximum number of retry attempts. Default: 5.
        sleep_time (int): Base sleep time in seconds between retries. Default: 5.

    Returns:
        tuple: (original_image, resized_image) as numpy arrays if successful, (None, None) if failure.
    """
    image = None
    image_resized = None
    attempts = 0
    
    while attempts < call_limit:
        try:
            image_data = None       
            while True:
                global allowed_connection_lock, allowed_connections_current
                condition = False
                with allowed_connection_lock:
                    if allowed_connections_current > 0:
                        allowed_connections_current -= 1
                        condition = True
                if condition:
                    image_data = await fetch_image(url, session)
                    break
                await asyncio.sleep(sleep_time)
                    
            if image_data is None:
                attempts += 1
                # Exponential backoff: sleep_time * 2^attempts
                backoff_sleep = sleep_time * (2 ** (attempts - 1))
                await asyncio.sleep(backoff_sleep)
                continue
            
            image = np.frombuffer(image_data, np.uint8)
            image = cv2.imdecode(image, cv2.IMREAD_COLOR)
            image_resized = cv2.resize(image, image_size)
            break
        except Exception as err:
            logging.warning(f"an unexpected error occurred: {url}:{err}")
            attempts += 1
    if isinstance(image, np.ndarray) and isinstance(image_resized, np.ndarray) and len(image) and len(image_resized):
        return image, image_resized
    return None, None
    
async def save_image(filepath_original, filepath_resized, image_org, image_resized, org_save_true=False):
    """
    Saves original and resized images to disk as PNG files.
    
    Args:
        filepath_original (str): Full path for the original image (including .png extension).
        filepath_resized (str): Full path for the resized image (including .png extension).
        image_org (np.ndarray): Original image as numpy array.
        image_resized (np.ndarray): Resized image as numpy array.
        org_save_true (bool): If False, skips saving the original image. Default: False.
    
    Returns:
        tuple: (success, exception_flag) where success is bool and exception_flag is True if any error occurred.
    """
    exception = True
    if isinstance(image_org, np.ndarray) and isinstance(image_resized, np.ndarray):
        try:    
            
            cond = [False,False]
            for index,(file, image) in enumerate(zip([filepath_original, filepath_resized],[image_org,image_resized])):
                if not org_save_true and index == 0:
                    cond[index] = True
                    continue
                cv2.imwrite(file, image)
                cond[index] = True
            if cond[0] and cond[1]:
                exception = False
                return True, exception
        except Exception as err:
            logging.warning(f"An error occurred while saving: {err}")
    else:
        logging.warning("Input images are not valid numpy arrays.")
    return False, exception
    
async def process_image(row, original_dir, resized_dir, download_args, session, org_save_true=False):
    """
    Downloads, processes, and saves a single image record.
    
    Fetches an image from the URL in the row, resizes it according to download_args,
    and saves both versions to disk. Records are expected to have 'id' and 'url' fields.

    Args:
        row (pd.Series): DataFrame row with at minimum 'id' and 'url' fields.
        original_dir (str): Directory path where original images are saved.
        resized_dir (str): Directory path where resized images are saved.
        download_args (dict): Parameters for image fetching (image_size, call_limit, sleep_time).
        session (aiohttp.ClientSession): Reused HTTP session.
        org_save_true (bool): If False, original images are not saved to disk. Default: False.

    Returns:
        tuple: ([name, id_, url], exception_flag) where exception_flag is True if any error occurred.
    """
    id_ = row['id']
    url = row['url']
    name = str(id_)
    
    exception = True
    returned = await get_image(url, session=session, **download_args)
    if returned[0] is not None and returned[1] is not None:
        img_original, img_resized = returned
        image_filepath_original = os.path.join(original_dir, f'{name}.png')
        image_filepath_resized = os.path.join(resized_dir, f'{name}.png')
        _, exception = await save_image(image_filepath_original, image_filepath_resized, img_original,img_resized, org_save_true)
    return [name, id_, url], exception

def create_tasks_in_generator(chunk, original_dir, resized_dir, image_size, call_limit, sleep_time, batch_size=1000):
    """
    Generator that yields batches of image processing tasks from a DataFrame chunk.
    
    Groups DataFrame rows into batches and yields task tuples containing the row data
    and processing parameters needed for async image download and processing.
    
    Args:
        chunk (pd.DataFrame): DataFrame slice containing image metadata (id, url columns).
        original_dir (str): Directory path for original images.
        resized_dir (str): Directory path for resized images.
        image_size (tuple): Target image size (width, height).
        call_limit (int): Max retry attempts per image.
        sleep_time (int): Base sleep duration for retries.
        batch_size (int): Number of images to group per batch. Default: 1000.
    
    Yields:
        list: Batch of tasks, where each task is (row, original_dir, resized_dir, download_args).
    """
    tasks = []
    download_args = {'image_size': image_size, 'call_limit': call_limit, 'sleep_time': sleep_time}
    for index, row in chunk.iterrows():
        tasks.append((row, original_dir, resized_dir, download_args))
        if len(tasks) >= batch_size:
            yield tasks
            tasks = []
    if tasks:
        yield tasks
    
async def process_tasks(chunk, original_dir, resized_dir, image_size, batch_size=1000, call_limit=5, sleep_time=5, org_save_true=False):
    """
    Asynchronously processes a chunk of image metadata records.
    
    Downloads, resizes, and saves images in batches. Tracks failed downloads
    and removes partially saved files if an error occurs.
    
    Args:
        chunk (pd.DataFrame): Metadata records with id and url columns.
        original_dir (str): Directory for storing original images.
        resized_dir (str): Directory for storing resized images.
        image_size (tuple): Target image dimensions (width, height).
        batch_size (int): Number of concurrent images per batch. Default: 1000.
        call_limit (int): Retry attempts per image. Default: 5.
        sleep_time (int): Base sleep time for retries in seconds. Default: 5.
        org_save_true (bool): Save original images if True. Default: False.
    """
    for tasks in create_tasks_in_generator(chunk, original_dir, resized_dir, image_size, call_limit, sleep_time, batch_size):
        async with aiohttp.ClientSession() as session:
            results = await asyncio.gather(
                *[process_image(task[0], task[1], task[2], task[3], session, org_save_true=org_save_true) for task in tasks],
                return_exceptions=True
            )
        for result in results:
            if result is None:
                continue
            if isinstance(result, Exception):
                logging.warning(f"Task failed with exception: {result}")
                continue
            image_info, exception = result
            if not exception:
                continue

            global missing_images_lock, missing_images, missing_image_ids
            with missing_images_lock:
                # Check if image already recorded as missing to avoid duplicates
                image_id = str(image_info[1])
                if image_id not in missing_image_ids:
                    missing_images.append({'id': image_id, 'url': image_info[2]})
                    missing_image_ids.add(image_id)

            name = str(image_info[0])
            image_filename_original = os.path.join(original_dir, f'{name}.png')
            image_filename_resized = os.path.join(resized_dir, f'{name}.png')

            for file in [image_filename_original, image_filename_resized]:
                if not org_save_true and file == image_filename_original:
                    continue
                if os.path.exists(file):
                    try:
                        os.remove(file)
                    except Exception as err:
                        logging.warning(f"an exception occurred while deleting a file: {err}")
    return
# ============================================================================
# Parallel Task Processing
# ============================================================================

def process_tasks_wrapper(chunk, task_args, download_args, batch_size=1000, org_save_true=False, windows=False):
    """
    Synchronous wrapper for async image processing in thread pool executor.
    
    Sets up event loop for async operations (with Windows compatibility) and
    delegates to process_tasks for actual image processing.

    Args:
        chunk (pd.DataFrame): Image metadata to process.
        task_args (dict): Directory arguments (original_dir, resized_dir).
        download_args (dict): Download parameters (image_size, call_limit, sleep_time, optional: allowed_connections).
        batch_size (int): Image batch size. Default: 1000.
        org_save_true (bool): Save original images. Default: False.
        windows (bool): Set Windows event loop policy if True. Default: False.
    """
    if windows:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(process_tasks(
        chunk,
        task_args['original_dir'],
        task_args['resized_dir'],
        download_args['image_size'],
        batch_size=batch_size,
        call_limit=download_args['call_limit'],
        sleep_time=download_args['sleep_time'],
        org_save_true=org_save_true
    ))
    return 

def orchestrate(metadata, missing_images_file, original_dir, resized_dir, 
                download_args, max_workers, batch_size, windows, org_save_true):
    """
    Orchestrates the image download and processing pipeline.
    
    Can be imported and called programmatically with pre-constructed paths and metadata.
    Flexible: accepts DataFrame directly, allowing data from any source (CSV, database, etc.)
    or with custom preprocessing applied.

    Args:
        metadata (pd.DataFrame): DataFrame with image metadata (must have 'id' and 'url' columns).
        missing_images_file (str): Path where failed image records will be written.
        original_dir (str): Directory for storing original images.
        resized_dir (str): Directory for storing resized images.
        download_args (dict): Parameters for image fetching (image_size, call_limit, sleep_time).
        max_workers (int): Number of parallel worker threads.
        batch_size (int): Number of images per async batch.
        windows (bool): Set Windows event loop policy if True.
        org_save_true (bool): Save original images if True.
    """
    global allowed_connections, allowed_connections_current, thread_stop, missing_images, missing_image_ids
    
    # Reset global state for each orchestration run
    allowed_connections = download_args.get('allowed_connections', 10000)
    allowed_connections_current = allowed_connections
    with missing_images_lock:
        missing_images.clear()
        missing_image_ids.clear()
    
    # Set up directories
    for dir_path in [original_dir, resized_dir, os.path.dirname(missing_images_file)]:
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
    
    task_args = {
        'original_dir': original_dir,
        'resized_dir': resized_dir
    }
    
    thread_stop = False
    sequences_thread = threading.Thread(target=write_missing_images, args=(missing_images_file,))
    sequences_thread.daemon = True
    sequences_thread.start()
    
    connections_thread = threading.Thread(target=monitor_connections, args=())
    connections_thread.daemon = True
    connections_thread.start()
    
    try:
        chunks = np.array_split(metadata, max_workers)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = executor.map(
                process_tasks_wrapper,
                chunks,
                [task_args] * len(chunks),
                [download_args] * len(chunks),
                [batch_size] * len(chunks),
                [windows] * len(chunks),
                [org_save_true] * len(chunks)
            )
        for result in results:
            pass
    finally:
        thread_stop = True
        sequences_thread.join()
        connections_thread.join()

    logging.warning(f"{len(missing_images)} images could not be downloaded")


if __name__ == '__main__':
    """
    Command-line entry point for distributed parquet-based processing.
    
    Loads configuration, discovers parquet files from tile-partitioned metadata,
    selects a chunk of files based on command-line indices, shuffles them randomly,
    then processes each file independently via orchestrate().
    
    Command-line arguments:
        chunk_idx (int): Index of the chunk to process (0-based).
        chunk_size (int): Number of files per chunk.
    
    Configuration reads from config.yaml:
        - paths.image_dir: Base directory for image storage
        - paths.tile_partitioned_parquet_raw_metadata_dir: Directory containing parquet files
        - image_params: image_size, call_limit, sleep_time, allowed_connections, max_workers, batch_size, windows, org_save_true, random_seed
    
    Each parquet file is processed independently, with failed downloads logged to:
        <image_dir>/logs/missing_<filename>.csv
    """
    logging.info("Starting image download script")
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    cfg = load_config()
    logging.info("Configuration loaded successfully")

    # Extract configuration parameters
    download_args = {k: cfg['image_params'][k] for k in ['image_size', 'call_limit', 'sleep_time', 'allowed_connections']}
    max_workers = cfg['image_params']['max_workers']
    batch_size = cfg['image_params']['batch_size']
    windows = cfg['image_params']['windows']
    org_save_true = cfg['image_params']['org_save_true']
    image_dir = os.path.abspath(cfg['paths']['image_dir'])
    
    random_seed = cfg['image_params']['random_seed']
    random.seed(random_seed)
    logging.info(f"Parameters: max_workers={max_workers}, batch_size={batch_size}, windows={windows}, org_save_true={org_save_true}, random_seed={random_seed}")

    tiles_dir = os.path.abspath(cfg['paths']['tile_partitioned_parquet_raw_metadata_dir'])
    all_files = glob.glob(os.path.join(tiles_dir, '*', '*.parquet'), recursive=True)
    if len(sys.argv) < 3:
        raise ValueError("Usage: image_download.py <chunk_idx> <chunk_size>")
    chunk_idx = int(sys.argv[1])
    chunk_size = int(sys.argv[2])
    all_files = sorted(all_files)
    random.shuffle(all_files)
    selected_files = all_files[chunk_idx*chunk_size:(chunk_idx+1)*chunk_size]
    logging.info(f"Processing chunk {chunk_idx} with chunk_size {chunk_size}, selected {len(selected_files)} files")

    for idx, file in enumerate(selected_files):
        logging.info(f"Processing file {idx + 1}/{len(selected_files)}: {os.path.basename(file)}")
        filename = os.path.basename(file)
        tile_name = os.path.basename(os.path.dirname(file))
        tile_image_dir = os.path.join(image_dir, tile_name)

        missing_images_filepath = os.path.join(image_dir, 'logs')
        missing_images_file = os.path.join(missing_images_filepath, f'missing_{filename.replace(".parquet", ".csv")}')
        
        original_dir = os.path.join(tile_image_dir, 'originals')
        resized_dir = os.path.join(tile_image_dir, 'resized')
        logging.info(f"Directories set: original_dir={original_dir}, resized_dir={resized_dir}, missing_images_file={missing_images_file}")
        # Load metadata from CSV
        metadata = pd.read_parquet(file)
        logging.info(f"Loaded {len(metadata)} image records from {filename}")
        
        # Call orchestration function
        orchestrate(
            metadata=metadata,
            missing_images_file=missing_images_file,
            original_dir=original_dir,
            resized_dir=resized_dir,
            download_args=download_args,
            max_workers=max_workers,
            batch_size=batch_size,
            windows=windows,
            org_save_true=org_save_true
        )
        logging.info(f"Completed processing {filename}")
    
    logging.info("Image download script completed successfully")


    
   
    
