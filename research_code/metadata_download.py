import math, os, time, logging, sys
from tqdm import tqdm
import asyncio, threading, aiohttp
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import box
from shapely import Point
import cProfile

logging.basicConfig(level=logging.WARNING)

#############################################################################################################
#############################################################################################################
"""------------------------------Monitoring Functions-------------------------------------------------------"""
#These functions here are used to monitor and save the data in between
# "monitor_jobs" monitors the number of scripts running at a given moment that are still querying bounding bboxes
# to adjust the number of requests a script can send per minute

# "write_sequences" and "write_bbox" run on a different thread and save the sequences and finished bboxes
# in between

# 'monitor_connections' sets the 'allowed_connection_current' to 'allowed_connection' each minute
# 'write_metadata_on_the_fly' and 'write_missing_sequences_on_the_fly' write the metadata and the sequences that could not
# be downloaded on the fly

def check_timeout_function(start, interval, check_timeout):
    if start:
        return -1
    for i in range(math.ceil(interval/check_timeout)):
        if thread_stop:
            return 0
        else:
            time.sleep(check_timeout)
            return 1

def monitor_jobs(condition_sequence, condition_metadata, dir,
                 interval=10,threshold_sequence=450,threshold_sequence_metadata=450,start=False,check_timeout=10, 
                 max_connections=10000):
    """Monitor directory for active sequence/metadata files and update globals.
    This is used to estimate how many jobs are currently running.

    Counts files in `dir` that start with `condition_sequence` or
    `condition_metadata` and were modified within their thresholds. Updates
    `number_of_jobs_running` and `allowed_connection`. Loops until the module-
    level flag `thread_stop` is True; if `start` is True performs one pass.
    """
    global thread_stop
    while not thread_stop:
        global allowed_connection,number_of_jobs_running

        #to check whether a sequence file is updated aka a job is running
        sequence_files = [file.split(".")[0].split("_")[-1] for file in os.listdir(dir) if file.startswith(condition_sequence)
            and (time.time() - os.path.getmtime(os.path.join(dir, file)) < threshold_sequence)]
        #to check whether metadata is updated aka a job is running
        metadata_files = [file.split(".")[0].split("_")[-1] for file in os.listdir(dir) if (file.startswith(condition_metadata))
            and (time.time() - os.path.getmtime(os.path.join(dir, file)) < threshold_sequence_metadata)]
        
        files = set(sequence_files).union(set(metadata_files))
        number_of_jobs_running = len(files)
        
        if number_of_jobs_running > 0:
            allowed_connection = round(max_connections/(number_of_jobs_running))
        else:
            allowed_connection = max_connections
        check_timeout_result = check_timeout_function(start, interval, check_timeout)
        if check_timeout_result == -1:
            break
        elif check_timeout_result == 0:
            return
        
def monitor_connections(interval=60,start=False,check_timeout=10):
    """"Monitor and reset allowed connections periodically.
    Sets `allowed_connection_current` to `allowed_connection` every `interval`
    seconds. Loops until the module-level flag `thread_stop` is True
    Overall, it is similar to 'mionitor_jobs' function, but updates the
    connections WITHIN a job"""
    global thread_stop
    while not thread_stop:
        global allowed_connection_lock,allowed_connection,allowed_connection_current
        with allowed_connection_lock:
            allowed_connection_current = allowed_connection
        check_timeout_result = check_timeout_function(start, interval, check_timeout)
        if check_timeout_result == -1:
            break
        elif check_timeout_result == 0:
            return
        
def write_sequences(sequence_file,interval=300,start=False,check_timeout=10):
    """Write global sequences to CSV periodically.
    Writes the current `global_sequences` to `sequence_file` every 'interval
    and loops until the module-level flag `thread_stop` is True.
    This is used to save progress periodically and not lose data if the process
    """
    global thread_stop,lock
    while not thread_stop:
        global lock
        with lock:
            global global_sequences
            sequences =  pd.DataFrame({'sequences':list(global_sequences)})
            sequences.to_csv(sequence_file,index=False)
        check_timeout_result = check_timeout_function(start, interval, check_timeout)
        if check_timeout_result == -1:
            break
        elif check_timeout_result == 0:
            return
        
def write_bbox(bbox_file,interval=300,start=False,check_timeout=10):
    """The same logic as 'write_sequences' but for bboxes"""
    global thread_stop, lock_bbox
    while not thread_stop:
        with lock_bbox:
            global global_bboxes
            bboxes = create_geodataframe_from_bboxes(list(global_bboxes))
            if len(bboxes):
                bboxes.to_file(bbox_file,driver='GPKG',index=False)
        check_timeout_result = check_timeout_function(start, interval, check_timeout)
        if check_timeout_result == -1:
            break
        elif check_timeout_result == 0:
            return

def write_data(custom_list_df, filepath):
    """Write a list of DataFrames to CSV, appending if file exists."""
    custom_list_df.reset_index(drop=True,inplace=True)
    if os.path.isfile(filepath):
        custom_list_df.to_csv(filepath,mode='a', header=False, index=False)
    else:
        custom_list_df.to_csv(filepath,index=False)

def flush_metadata_buffer(filepath):
        """Write metadata_list to CSV if non-empty, clearing the buffer in-place."""
        global metadata_lock, metadata_list
        with metadata_lock:
            if metadata_list:
                metadata_list_df = pd.concat(metadata_list)
                write_data(metadata_list_df, filepath)
                metadata_list.clear()

def flush_missing_sequences_buffer(filepath):
    """Write missing_sequences_list to CSV if non-empty, clearing the buffer in-place."""
    global missing_sequences_lock, missing_sequences_list
    with missing_sequences_lock:
        if missing_sequences_list:
            missing_sequences_list_df = pd.DataFrame({'sequence':missing_sequences_list})
            write_data(missing_sequences_list_df, filepath)
            missing_sequences_list.clear()

def write_data_on_the_fly(filepath, function, end=False,interval=300,check_timeout=10):
    """Write data on the fly periodically. The function it takes is either ''flush_metadata_buffer'
      or 'flush_missing_sequences_buffer' depending on the type of data to be written."""
    global write_true
    while write_true:
        function(filepath)
        if end:
            break
        for i in range(math.ceil(interval/check_timeout)):
            if not write_true:
                function(filepath)
                return
            else:
                time.sleep(check_timeout)
                
#############################################################################################################
#############################################################################################################
"""---------------------------API Functions--------------------------------------"""
async def data_handling(url, attempt, empty_data_attempts, is_data_empty, sleep_time=5, max_connections=10000):
    json = None
    try:
        while True:
            condition = False
            global allowed_connection_lock
            with allowed_connection_lock:
                global allowed_connection_current
                if allowed_connection_current:
                    allowed_connection_current -= 1
                    condition = True
            if condition:        
                json = await async_get_response(url)
                break
            else:
                await asyncio.sleep(sleep_time)    
    except Exception as e:
        logging.warning(f'an error happened while retrieving images: {url}:{e}')
        
    global number_of_jobs_running
    factor = number_of_jobs_running if number_of_jobs_running != 0 else 1
    factor *= allowed_connection_current if allowed_connection_current != 0 else 1
    delay_between_requests = round(60/max_connections/factor,5)

    if json is None:
        attempt += 1
        #logging.warning('no server response')
        await asyncio.sleep(delay_between_requests)
        return None, attempt, empty_data_attempts, is_data_empty, delay_between_requests
    
    data = json.get('data')
    del json
    if data is None:
        logging.warning('unexpected json format')
        attempt += 1
        await asyncio.sleep(delay_between_requests)
        return None, attempt, empty_data_attempts, is_data_empty, delay_between_requests
    
    data = pd.DataFrame(data)
    if  data.empty:
        if is_data_empty == empty_data_attempts:
            return None, attempt, empty_data_attempts, is_data_empty, delay_between_requests
        
        is_data_empty += 1
        await asyncio.sleep(delay_between_requests)
        return None, attempt, empty_data_attempts, is_data_empty, delay_between_requests
    
    return data, attempt, empty_data_attempts, is_data_empty, delay_between_requests

async def async_get_response(url):
    """Asynchronously get JSON response from URL using aiohttp."""
    try:
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url) as response:
                    global number_of_requests
                    number_of_requests += 1

                    if response is None:
                        logging.warning(f'no response for url: {url}')
                        return None
                    
                    json = await response.json()
                    
                    if response.status != 200:
                        logging.warning(f'not a valid response while trying to reach the server, status: {response.status}')
                        logging.warning(f'{json} : {url}')
                        return None
                    
                    if isinstance(json, dict) and json.get('error'):
                        error = json.get('error')
                        msg = error.get('message') if isinstance(error, dict) else str(error)
                        msg = msg if msg is not None else str(error)
                        logging.warning(f'API error for {url}: {msg}')
                        return None
                    elif json is None:
                        logging.warning(f'no json response for url: {url}')
                        return None
                    
                    return json
            except Exception as e:
                #logging.warning(f'an error occurred while extracting a response: {e}')      
                return None
    except Exception as e:  
        logging.warning(f'an error occurred while creating a session: {e}')      
        return None

async def adding_images_to_each_other(sequence, url, call_limit=5,empty_data_attempts=3,
                                      retries=5, sleep_time=5, max_connections=10000):

    metadata_images_global = []
    set_ = set() #to check if there are new ids added from each call
    attempt = 0
    is_data_empty = 0
    tracker = 0
    successful_attempt = 0
    
    while attempt < call_limit:
        data, attempt, empty_data_attempts, is_data_empty, delay_between_requests = await data_handling(url, empty_data_attempts,
                                                                                 is_data_empty, sleep_time,
                                                                                   max_connections)
        if data is None:
            continue

        #Extracting meaningful columns and getting the unique ids
        df_filtered = data[data.apply(lambda x: all(k in x for k in ["id", "thumb_original_url", "computed_geometry", "captured_at"]), axis=1)]
        df_filtered = df_filtered[~df_filtered['id'].isin(set_)]
        
        if not df_filtered.empty: #checking whether there are new ids
            metadata_images_global.append(df_filtered) #uploading the global list with unique ids
            set_.update(df_filtered['id'].tolist()) #adding unique ids to the set
            tracker = 0 ##resetting the call_limit_images if new images have been found
        else:
            tracker += 1
        
        if tracker == retries:
            break
        if successful_attempt == 0 and len(data) < 2000: #This allows us to abort in the first round if we can be sure
            break                                      #that the sequence contains less than 2000 elements 
        successful_attempt += 1
            
    if len(metadata_images_global):
        return pd.concat(metadata_images_global,ignore_index=True), None
    else:
        return pd.DataFrame(), sequence
   
async def process_one_sequence(sequence, mly_key, columns, args_dict):
    df_data = pd.DataFrame(columns=columns)
    url = f"""https://graph.mapillary.com/images?access_token={mly_key}&sequence_ids={[str(sequence)]}&
    fields={','.join([x for x in columns if x != 'sequence'])}"""   

    meta_data_images, sequence_info = await adding_images_to_each_other(sequence, url, **args_dict)
    if len(meta_data_images):
        df_data = pd.DataFrame(meta_data_images)
        df_data['sequence'] = sequence
        df_data['geometry'] = df_data['computed_geometry'].apply(lambda x: Point(x.get('coordinates')) if isinstance(x, dict) else np.nan)
        df_data['long'] = df_data['computed_geometry'].apply(lambda x: x.get('coordinates')[0] if isinstance(x, dict) else np.nan)
        df_data['lat'] = df_data['computed_geometry'].apply(lambda x: x.get('coordinates')[1] if isinstance(x, dict) else np.nan)
        
        for col in columns:
            if col not in df_data.columns:
                if col == "is_pano":
                        df_data[col] = False
                else:
                    df_data[col] = np.nan
                    
        df_data.drop(['computed_geometry'], inplace=True, axis=1)
        
    else:
        df_data.rename({'computed_geometry':'geometry'}, inplace=True, axis=1)
        
    df_data.rename({'thumb_original_url': 'url', 'captured_at': 'timestamp'}, axis=1, inplace=True)
    df_data = df_data[['sequence', 'id', 'url', 'long', 'lat', 'geometry',
                       'height', 'width', 'altitude', 'make', 'model',
                      'creator', 'is_pano', 'timestamp']]
    return df_data, sequence_info

"""------------------------------Division of bboxes-------------------------------------------------------"""        
# These functions divide a given bbox into smaller bboxes. 
# "divide_bbox" into 2 along the mid latitude
#  "segmented_bboxes" divides a given bbox into n x n  bboxes when provided with a number n
# 'create_geodataframe_from_bboxes' creates a GeoDataFrame from a list of bounding bboxes

def segmented_bboxes(boundary_box, n):
    west, south, east, north = boundary_box
    # Divide into ceil(sqrt(n)) times ceil(sqrt(n)) smaller boundary boxes
    num_rows_cols = math.ceil(math.sqrt(n))
    boxes = []
    for i in range(num_rows_cols):
        for j in range(num_rows_cols):
            sub_box_west = round(west + i * (east - west) / num_rows_cols,5)
            sub_box_east = round(west + (i + 1) * (east - west) / num_rows_cols,5)
            sub_box_south = round(south + j * (north - south) / num_rows_cols,5)
            sub_box_north = round(south + (j + 1) * (north - south) / num_rows_cols,5)
            
            sub_box = [sub_box_west, sub_box_south, sub_box_east, sub_box_north]
            boxes.append(sub_box)
    return boxes

def divide_bbox(bbox):
    west, south, east, north = bbox
    latitude_midpoint = round((south + north) / 2,5)
    bbox1 = [west, south, east, latitude_midpoint]
    bbox2 = [west, latitude_midpoint, east, north]
    return [bbox1, bbox2]

def create_geodataframe_from_bboxes(bboxes):
    polygons = []
    for bbox in bboxes:
        west, south, east, north = bbox
        polygons.append(box(west, south, east, north))
    gdf = gpd.GeoDataFrame({'geometry': polygons})
    return gdf

#############################################################################################################
#############################################################################################################
"""------------------------------Getting needed bboxes and unique sequences-------------------------------------------------------"""
# "generator_get_bboxes_and_sequences" is a generator that yields a bbox. When provided with a bbox, it sends a request
# to the server. If the response includes 2000 elements, 2000 being the max number of elements the server returns with a request
# ,the function will divide the bbox into n x n bboxes till a bbox is reached that returns less than 2000 elements
# it will calculate the number of requests per minute between requests based on the global variable 'number_of_jobs_running'.
# However, in this case, a logic must be implemented that takes these variables into account. "monitor_jobs" function
# could do this if it is run on another thread
# it increments the the globals sets 'global_sequences' and 'global_boxes' with the bboxes that are successfully queried
#  and with sequences that could be found. These sets will be returned eventually.

#'generator_get_bboxes_and_sequences' will be run in 'get_bboxes_and_sequences' as more bboxes are finished
# the logic of 'get_bboxes_and_sequences' is the same as 'generator_get_bboxes_and_sequences'. Use 'get_bboxes_and_sequences'
# since 'get_bboxes_and_sequences' is an asynchronous function, it must be wrapped around if one wants to call it from a synchronous function
# this is the reason why 'get_bboxes_and_sequences_wrapping' exists. if you want to run this function from a Windows system,
# set 'windows' to True

# 'get_sequences' orchestrates the whole process using a ThreadPoolExecutor. The bbox will initially be divided into
# n x n sub bboxes and each of these sub_bboxes will be run in a single thread. At a given time maximum of 
# max_workers threads will be running. If you do not want to use parallelization, just set 'max_workers' to 1. 

async def generator_get_bboxes_and_sequences(bbox, n, mly_key, call_limit=5, empty_data_attempts=3, 
                                             sleep_time=5, max_connections=10000):
    bbox_str = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"
    url = f"https://graph.mapillary.com/images?access_token={mly_key}&fields=sequence&bbox={bbox_str}"
    
    data = None
    attempt = 0
    is_data_empty = 0

    while attempt < call_limit:
        data, attempt, empty_data_attempts, is_data_empty, delay_between_requests = await data_handling(url, empty_data_attempts,
                                                                                 is_data_empty, sleep_time,
                                                                                   max_connections)
        if data is None:
            continue
        if 'sequence' not in data.columns:
            attempt += 1
            logging.warning('sequence not in data')
            await asyncio.sleep(delay_between_requests)
        break
        
    if data is None or data.empty or 'sequence' not in data.columns:
        yield None
        return 
    
    sequence_set = set()
    sequence_set = set(data['sequence'].unique())
    if sequence_set:
        global lock
        with lock:
            global global_sequences
            global_sequences = global_sequences.union(sequence_set)
            
    if len(data) == 2000:
        del data
        sub_bboxes = segmented_bboxes(bbox, n)
        for sub_bbox in sub_bboxes:
            async for _ in generator_get_bboxes_and_sequences(sub_bbox, n, mly_key, call_limit,
                                                               empty_data_attempts, sleep_time, max_connections):
              yield None
    else:
        del data
        bbox_set = set()
        bbox_set.add(tuple(bbox))
        if bbox_set:
            global lock_bbox
            with lock_bbox:
                global global_bboxes
                global_bboxes = global_bboxes.union(bbox_set)
        yield None
            
async def get_bboxes_and_sequences(bbox, n, mly_key, args):        
    async for _ in generator_get_bboxes_and_sequences(bbox, n, mly_key, **args):
        pass
    return

def get_bboxes_and_sequences_wrapping(bbox, n, mly_key, args, windows=False):
    if windows:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(get_bboxes_and_sequences(bbox, n, mly_key, args))
    return

def get_sequences(bbox_, mly_key, n, dir, number_of_initial_bboxes,
                args, id_=0, max_workers=4, windows=False):
    
    #Define filepaths and prefixes, remove possible files, call the monitor functions initially to start them
    #Calling the monitoring functions once serves the purpose of setting the number of jobs running correctly
    #and calculating the number of requests per minute for a single job

    if not os.path.exists(dir):
        os.makedirs(dir, exist_ok=True)

    
    sequence_file = os.path.join(dir, f'sequences_{id_}.csv')
    finished_bboxes = os.path.join(dir, f'finished_bboxes_{id_}.gpkg')
  
    condition_sequence = f'sequences_'
    condition_metadata = f'metadata_unfiltered_'
    
    for file in [sequence_file,finished_bboxes]:
        if os.path.exists(file):
            os.remove(file)
    
    global thread_stop
    thread_stop = False
    
    write_sequences(sequence_file,start=True)
    write_bbox(finished_bboxes,start=True)
    monitor_jobs(condition_sequence,condition_metadata,dir,start=True)
    monitor_connections(start=True)
    
    ####################################################################################################
    ####################################################################################################
    #Now start monitoring threads
    sequences_thread = threading.Thread(target=write_sequences, args=(sequence_file,))
    sequences_thread.daemon = True
    sequences_thread.start()
    
    bboxes_thread = threading.Thread(target=write_bbox, args=(finished_bboxes,))
    bboxes_thread.daemon = True
    bboxes_thread.start()
    
    connections_thread = threading.Thread(target=monitor_connections, args=())
    connections_thread.daemon = True
    connections_thread.start()
    
    job_thread = threading.Thread(target=monitor_jobs, args=(condition_sequence,condition_metadata,dir))
    job_thread.daemon = True
    job_thread.start()
    
    ####################################################################################################
    ####################################################################################################
    #Now we can start    
    
    bboxes = segmented_bboxes(bbox_, number_of_initial_bboxes)
    print(len(bboxes))
    
    #this here is to check how many requests have been set to query
    global number_of_requests
    number_of_requests_initial = number_of_requests
    t1 = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        list(executor.map(get_bboxes_and_sequences_wrapping,
                                    bboxes,
                                    [n]*len(bboxes),
                                    [mly_key]*len(bboxes),
                                    [args]*len(bboxes),
                                    [windows]*len(bboxes)))    
    ####################################################################################################
    ####################################################################################################
    #Now we stop the threads, write the files and create statistics
    t2 = time.time()    
    thread_stop = True #to stop the monitoring steps since the query is finished
    global global_sequences, global_bboxes
    number_of_requests_final = number_of_requests
    number_of_requests_for_query = number_of_requests_final-number_of_requests_initial
    average_time = (t2-t1)/(number_of_requests_final-number_of_requests_initial)
    print(f"number of sequences found: {len(global_sequences)}")
    print(f'it took {round(t2-t1,2)} seconds to query')
    print(f'{number_of_requests_for_query} responses have been sent within {round(t2-t1,2)} seconds')
    print(f'average time for a response to be processed: {round(average_time,2)} seconds')  
    
    global_sequences_dataframe = pd.DataFrame({'sequences':list(global_sequences)})
    global_sequences_dataframe.to_csv(sequence_file,index=False)
        
    bboxes = create_geodataframe_from_bboxes(list(global_bboxes))
    if len(bboxes):
        bboxes.to_file(finished_bboxes,driver='GPKG',index=False)
        
    sequences_thread.join()
    bboxes_thread.join()
    connections_thread.join()
    job_thread.join()
    
    return list(global_bboxes),list(global_sequences)

#############################################################################################################
#############################################################################################################        
"""------------------------------Getting the metadata-------------------------------------------------------"""
# The logic here is similar to the one provided with querying bounding bboxes. 
# 'get_metadata' will orchestrate the process and will be implemented synchronously. When provided with a sequence list
# # the sequence list will be divided into 'max_workers' subarrays that will be run on different threads using a ThreadPoolExecutor
# if you do not want to use parallelization you can set 'max_workers' to 1. On each thread, an instance of 'metadata_download_wrapping'
# will run. This is again a synchronous function that creates an eventloop and run the actual asynchronous function 'metadata_download'
# in it 'metadata_download' will create tasks on for the eventloop and run them in batches using a generator. sequences will be gone
# through one by one and be downloaded using 'async_process_one_sequence.

# "get_metadata" will write two files to the directory provided combining the 'continent' and 'grid' as filenames.
# It uses the functions 'write_missing_sequences_on_the_fly' and 'write_metadata_on_the_fly' periodically to spare memory. 
# The files will have the format: f'{dir}/metadata_unfiltered_{continent}_{grid}.csv'
#  and f'{dir}/missing_sequences_{continent}_{grid}.csv'

def process_generator(sequences, batch_size=100):
    batch_sequences = []
    for sequence in sequences:
        batch_sequences.append(sequence)
        if len(batch_sequences) == batch_size:
            yield batch_sequences
            batch_sequences = []  # Reset batch_sequences for next batch
    if batch_sequences:  # Yield the last batch
        yield batch_sequences
            
async def metadata_download(mly_key, sequences, columns, args_dict, batch_size=100):
    
    for batch_sequences in tqdm(process_generator(sequences,batch_size)):
        
        tasks = [process_one_sequence(sequence,mly_key, columns, args_dict) 
                 for sequence in batch_sequences if sequence is not None]
        results = await asyncio.gather(*tasks)
        results = [result for result in results if result is not None]
        for result in results:
            if result[0] is not None:
                global metadata_lock
                with metadata_lock:
                    global metadata_list
                    metadata_list.append(result[0])
                    
            if result[1] is not None:
                global missing_sequences_lock
                with missing_sequences_lock:
                    global missing_sequences_list
                    missing_sequences_list.append(result[1])
        del results   
    return 

def metadata_download_wrapping(mly_key,sequences,columns, args_dict, batch_size=100, windows=False):
    if windows:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(metadata_download(mly_key, sequences, columns, args_dict, batch_size))
    return

def get_metadata(sequence_list, missing_sequences, file_unfiltered_metadata, mly_key, columns, args_dict, 
                 max_workers=4, batch_size=100, windows=False):
   
    dir = os.path.dirname(file_unfiltered_metadata)

    if not os.path.exists(dir):
        os.makedirs(dir, exist_ok=True)

    condition_sequence = f'sequences__'
    condition_metadata = f'metadata_unfiltered_'
    #############################################################################################################
    #############################################################################################################
    #Start threads
    global allowed_connection_current,allowed_connection
    allowed_connection_current = allowed_connection
    global thread_stop
    thread_stop = False
    monitor_jobs(condition_sequence,condition_metadata,dir,start=True)
    monitor_connections(start=True)
    job_thread = threading.Thread(target=monitor_jobs, args=(condition_sequence,condition_metadata,dir))
    job_thread.daemon = True
    job_thread.start()
    
    connections_thread = threading.Thread(target=monitor_connections, args=())
    connections_thread.daemon = True
    connections_thread.start()
    
    global write_true
    write_true = True
    metadata_thread = threading.Thread(target=write_data_on_the_fly, args=(file_unfiltered_metadata, flush_metadata_buffer))
    metadata_thread.daemon = True
    metadata_thread.start()
    
    missing_sequences_thread = threading.Thread(target=write_data_on_the_fly, args=(missing_sequences, flush_missing_sequences_buffer))
    missing_sequences_thread.daemon = True
    missing_sequences_thread.start()
    
    #############################################################################################################
    #############################################################################################################
    #Run the process
    global number_of_requests 
    number_of_requests_initial = number_of_requests
    
    t2 = time.time()
    sequence_list_chunks = np.array_split(sequence_list,max_workers)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        list(executor.map(metadata_download_wrapping,
                [mly_key]*len(sequence_list_chunks),
                sequence_list_chunks,
                [columns]*len(sequence_list_chunks),
                [args_dict]*len(sequence_list_chunks),
                [batch_size]*len(sequence_list_chunks),
                [windows]*len(sequence_list_chunks)))
    
    thread_stop = True
    write_true = False
    
    #statistics about the download
    t3 = time.time()
    number_of_requests_final = number_of_requests
    number_of_requests_for_downloads = number_of_requests_final-number_of_requests_initial
    average_time = (t3-t2)/(number_of_requests-number_of_requests_initial)
    print(f'it took {round(t3-t2,2)} seconds to download')
    #print(f"number of images downloaded: {len(whole_metadata)}")
    print(f'{number_of_requests_for_downloads} responses have been sent within {round(t3-t2,2)} seconds')
    print(f'average time for a response to be downloaded: {round(average_time,2)} seconds')
    
    write_data_on_the_fly(file_unfiltered_metadata, flush_missing_sequences_buffer, end=True)
    write_data_on_the_fly(missing_sequences, flush_missing_sequences_buffer, end=True)
    
    connections_thread.join()
    job_thread.join()
    missing_sequences_thread.join()
    metadata_thread.join()
    return
#############################################################################################################
#############################################################################################################
"""------------------------------Et Actio-------------------------------------------------------"""
#This funcrion should be updated. it is quite old. I cannot guarentee that it will work
async def missing_sequences_download(sequence_list,mly_key,columns,file_unfiltered_metadata,missing_sequences,call_limit_images=10,call_limit=5,base_limit=1,max_workers=4,async_true=True):
    """
    Download missing sequences metadata and save them to CSV files.

    Args:
        sequence_list (list): List of Mapillary sequence IDs for which the download failed, they could be retrained from the CSV files
                              starting with "missing_sequence_".
        mly_key (str): Mapillary access token.
        file_unfiltered_metadata (str): Path to save unfiltered metadata CSV file.
        missing_sequences (str): CSV file to save still missing sequences
        call_limit_images (int, optional): Maximum number of API call attempts per sequence
                                        when the server returns 2000 (API limit) images in each turn. Defaults to 10.
        call_limit (int, optional): Maximum number of API call attempts per sequence when no data is returned. Defaults to 5.
    """
    task = [get_metadata(sequence_list,mly_key, columns, call_limit_images,call_limit,base_limit,max_workers,async_true)]
    
    
    results = await asyncio.gather(*task)
    del task
    unfiltered_dfs = results[0][0]
    sequences_info = results[0][1]
    del results
    
    if len(unfiltered_dfs):
        if not isinstance(unfiltered_dfs,pd.DataFrame):
            unfiltered_dfs = pd.concat(unfiltered_dfs)
        if os.path.exists(file_unfiltered_metadata):
            file_to_be_concatenated = pd.read_csv(file_unfiltered_metadata)
            unfiltered_dfs = pd.concat([file_to_be_concatenated,unfiltered_dfs],axis=0,ignore_index=True)

        unfiltered_dfs.reset_index(drop=True,inplace=True)
        unfiltered_dfs.to_csv(file_unfiltered_metadata,index=False)
        unfiltered_dfs = None
    if len(sequences_info):
        sequences_info = np.unique(np.array(sequences_info))
        sequences_info = pd.DataFrame({"sequence":sequences_info})
        sequences_info.to_csv(missing_sequences,index=False)
        
def main(bbox_, mly_key, columns, n, dir, id_, file_unfiltered_metadata, missing_sequences, 
         number_of_initial_bboxes, args, max_workers=4, download_true=True, batch_size=100,windows=False):

    time_initial = time.time()
    file_unfiltered_metadata = os.path.join(dir, f'{file_unfiltered_metadata}.csv')
    missing_sequences = os.path.join(dir, f'{missing_sequences}.csv')
    
    for file in [file_unfiltered_metadata,missing_sequences]:
        if os.path.exists(file):
            os.remove(file)

    bboxes_list, sequence_list = get_sequences(bbox_,mly_key,n, dir,
                                               number_of_initial_bboxes, {k:v for k, v in args.items()
                                                                          if k != 'retries'}, id_,
                                              max_workers,windows)
    if download_true == True:   
        get_metadata(sequence_list, missing_sequences,file_unfiltered_metadata,
                     mly_key,columns,args,max_workers, batch_size,windows)
    #statistics about the whole process
    global number_of_requests
    total_requests = number_of_requests
    total_time = round(time.time()-time_initial,2)
    average_time = round(total_time/total_requests,2)
    print(f"total number of request:{total_requests}")
    print(f"total time passed:{total_time}")
    print(f'average time for a response to be downloaded: {round(average_time,2)} seconds')
    
#######################################################################################################################################
#######################################################################################################################################
#######################################################################################################################################
      
"""-------------------------------------------Parameters--------------------------------------"""
sys.setrecursionlimit(20000)
max_cpu_workers = int(os.environ.get('SLURM_CPUS_PER_TASK', os.cpu_count()))
max_workers = 4*max_cpu_workers
number_of_initial_bboxes = 100
n = 4
args = {
    'call_limit' : 5,
    'empty_data_attempts' : 3,
    'retries' : 5,
    'max_connections' : 10000,
    'sleep_time' : 5
}
columns = ['sequence', 'id', 'thumb_original_url', 'long', 'lat','computed_geometry', 'height', 'width', 'altitude','make', 'model', 'creator', 'is_pano', 'captured_at']

batch_size = 100
number_of_jobs_running = 1
number_of_requests = 0
download_true = True
windows = True
thread_stop = True
write_true = False

mly_keys = [
    "MLY|6700072406727486|6534c2cab766ff1eaa4c4defeb21a3f4",
    'MLY|6704446686232389|295d77211da2365c42ea656e7ab032c6',
    'MLY|7160741587366125|36727dbc605d960f172712bcbcaa3010',
    'MLY|24934087266239487|283a94b3c68c0c95f486cf4c11a229c2',
    'MLY|25723892110543686|a2cf5b92cf0b1126075958cc00159db6',
    'MLY|24901690569476887|16afd3356dd8d13bb2714075d42da6dd'
    ]

## Definition and Initialization of locks of global 
metadata_list = []
missing_sequences_list = []
global_sequences = set()
global_bboxes = set()

metadata_lock = threading.Lock()
missing_sequences_lock = threading.Lock()
lock = threading.Lock()
lock_bbox = threading.Lock()
allowed_connection_lock = threading.Lock()
allowed_connection = round(args['max_connections']/(number_of_jobs_running))
allowed_connection_current = args['max_connections']

if __name__ == '__main__':
    id_ = str(sys.argv[1]) if len(sys.argv) > 1 else 0
    dir = sys.argv[2] if len(sys.argv) > 2 else os.path.dirname(__file__)
    os.chdir(dir)
    dir = '../data/processed/mapillary_metadata'
    if not os.path.exists(dir):
        os.makedirs(dir, exist_ok=True)

    mly_key = mly_keys[int(sys.argv[3])] if len(sys.argv) > 3 else mly_keys[0]

    bbox = [8.657570, 49.392653, 8.707523, 49.420689]
    file_unfiltered_metadata, missing_sequences = f'metadata_unfiltered_{id_}',f'missing_sequences_{id_}'

    
    main(bbox,mly_key,columns,n,dir,id_,file_unfiltered_metadata,missing_sequences,
         number_of_initial_bboxes,args,max_workers,download_true,batch_size,windows)

    #cProfile.run("""main(bbox,mly_key,columns,n,dir,id_,file_unfiltered_metadata,missing_sequences,
    #             number_of_initial_bboxes,max_workers,download_true,batch_size,windows)""")