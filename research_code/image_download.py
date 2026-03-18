#!/usr/bin/env python
# coding: utf-8
import os,logging,time,math,sys
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import asyncio, aiohttp, threading
import cv2
from start import load_config

logging.basicConfig(level=logging.WARNING)

def monitor_connections(interval=60, start=False, check_timeout=10):
    global thread_stop, lock, allowed_connections, allowed_connections_current
    while not thread_stop:
        with allowed_connection_lock:
            allowed_connections_current = allowed_connections
        if start:
            break
        for i in range(math.ceil(interval/check_timeout)):
            if thread_stop:
                return
            else:
                time.sleep(check_timeout)
        
def write_missing_images(missing_images_file,interval=15, start=False, check_timeout=10):
    global thread_stop, missing_images_lock, missing_images
    while not thread_stop:
        with missing_images_lock:
            images =  pd.DataFrame(missing_images)
            if len(images):
                images.to_csv(missing_images_file, index=False)
        if start:
            break
        for i in range(math.ceil(interval/check_timeout)):
            if thread_stop:
                images =  pd.DataFrame(missing_images)
                if len(images):
                    images.to_csv(missing_images_file, index=False)
                return
            else:
                time.sleep(check_timeout)
                
"""-------------------------------------Image Download and Processsing----------------------"""
async def fetch_image(url):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as response:
                image_data = None
                if response.status == 200:
                    image_data = await response.read()
                    if isinstance(image_data, bytes):
                        if len(image_data):
                            return image_data
                        else:
                            logging.warning(f"Empty image data received from {url}")
                            logging.warning(image_data) 
                    else:
                        logging.warning(f"a problem happened while reading from the {url}")
                        logging.warning(image_data) 
                else:
                    logging.warning(response.status)
                    logging.warning(f"Failed to fetch image from {url}. Status code: {response.status}")
        except Exception as err: 
            #pass
            logging.warning(f"An exception occurred while fetching the image: {url}:{err}")
    return None
  
async def get_image(url, image_size, call_limit=5, sleep_time=5, allowed_connections=10000):
    """
    Fetches an image from a given URL and resizes it.

    Args:
    - url (str): The URL from which to fetch the image.
    - size (tuple): A tuple specifying the desired height and width of the image after resizing.
    - call_limit (int): The maximum number of retries allowed for fetching and resizing the image.

    Returns:
    - tuple: A tuple containing the original image and the resized image. If fetching or resizing fails,
             returns (None, None).
    """
    image = None
    image_resized = None
    attempts = 0
    
    while attempts < call_limit:
        try:
            image_data = None       
            while True:
                global lock, allowed_connections_current
                condition = False
                with lock:
                    if allowed_connections_current > 0:
                        allowed_connections_current -= 1
                        condition = True
                if condition:
                    image_data = await fetch_image(url)
                    break
                else:
                    await asyncio.sleep(sleep_time)
                    
            if image_data is None:
                attempts += 1
                if allowed_connections_current:
                    await asyncio.sleep(round(60/allowed_connections_current,5))
                else:
                    await asyncio.sleep(round(60/allowed_connections,5))
                continue
            
            image = np.frombuffer(image_data, np.uint8)
            image = cv2.imdecode(image, cv2.IMREAD_COLOR)
            image_resized = cv2.resize(image, image_size)
            break
        except Exception as err:
            logging.warning(f"an unexpected error occured: {url}:{err}")
            attempts += 1
            
    if isinstance(image, np.ndarray) and isinstance(image_resized, np.ndarray) and len(image) and len(image_resized):
        return image, image_resized
    else:
        return None, None
    
async def save_image(name_org, name_resized, image_org, image_resized, org_save_true=False):
    exception = True
    if isinstance(image_org, np.ndarray) and isinstance(image_resized, np.ndarray):
        try:    
            image_org_path = f'{name_org}.jpg'
            image_resized_path = f'{name_resized}.jpg'
            
            cond = [False,False]
            for index,(file,image) in enumerate(zip([image_org_path,image_resized_path],[image_org,image_resized])):
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
    
async def process_image(row, original_dir, resized_dir, download_args, org_save_true=False):
    """
    Process images from a DataFrame, resize them, and save the original and resized versions.

    Args:
    - df (pd.DataFrame): The DataFrame containing image information.
    - original_dir (str): The directory path where original images will be saved.
    - resized_dir (str): The directory path where resized images will be saved.
    - image_size (tuple): A tuple specifying the desired height and width of the resized image.

    Returns:
    - list: A list of boolean values indicating whether each image was processed and saved successfully.
    """
    id_ = row['id']
    url = row['url']
    name = str(id_)
  
    condition = False
    exception = True
    
    returned = await get_image(url, **download_args)
    if returned[0] is not None and returned[1] is not None:
        img_original, img_resized = returned
        image_filename_original = os.path.join(original_dir,name)
        image_filename_resized = os.path.join(resized_dir,name)
        condition, exception = await save_image(image_filename_original, image_filename_resized, img_original,img_resized, org_save_true)
    return [name, id_, url], exception

def create_tasks_in_generator(chunk, task_args, batch_size=1000):
    tasks = []
    for index, row in chunk.iterrows():
        tasks.append((row, task_args))
        if len(tasks) >= batch_size:
            yield tasks
            tasks = []
    if tasks:
        yield tasks
    
async def process_tasks(chunk,original_dir,resized_dir,image_size,batch_size=1000,call_limit=5,org_save_true=False):
    for tasks in create_tasks_in_generator(chunk,original_dir,resized_dir,image_size,batch_size):
        results = await asyncio.gather(*[process_image(*task, call_limit,org_save_true) for task in tasks])
        global number_of_elements_lock,number_of_elements
        
        with number_of_elements_lock:
             number_of_elements += len(tasks)

        for result in results:
            if result is None: continue
            image_info, exception = result
            if not exception: continue

            global missing_images_lock, missing_images
            with missing_images_lock:
                missing_images.append({'id':str(image_info[1]),'url':image_info[2]})
            
            name = str(image_info[0])
            image_filename_original = os.path.join(original_dir, f'{name}.jpg')
            image_filename_resized = os.path.join(resized_dir, f'{name}.jpg')
            
            for file in [image_filename_original,image_filename_resized]:
                if not org_save_true and file == image_filename_original:
                    continue
                if os.path.exists(file):
                    try:
                        os.remove(file)
                    except Exception as err:
                        logging.warning(f"an exception occurred while deleting a file: {err}")
    return

def process_tasks_wrapper(chunk, task_args, download_args, batch_size=1000, org_save_true=False, windows=False):
    if windows:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(process_tasks(chunk, task_args, download_args, batch_size=batch_size, org_save_true=org_save_true))
    return 

"""-------------------------------------Et Actio----------------------"""
def main(metadata_filepath, missing_images_file):
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    cfg = load_config()
    chunk = sys.argv[1]
    image_dir = (cfg['paths ']['image_dir'], chunk.split('.')[0])
    original_dir = os.path.join(image_dir,'originals')
    resized_dir = os.path.join(image_dir,'resized')
    task_args = {
        'original_dir': original_dir,
        'resized_dir': resized_dir
        } 
    download_args = {k: cfg['image_params'][k] for k in ['image_size', 'call_limit', 'sleep_time', 'allowed_connections']}
    max_workers = cfg['image_params']['max_workers']
    batch_size = cfg['image_params']['batch_size']
    windows = cfg['image_params']['windows']
    org_save_true = cfg['image_params']['org_save_true']
    global allowed_connections 
    allowed_connections = cfg['image_params']['allowed_connections']

    for dir in [original_dir, resized_dir]:
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
    
    t1 = time.time()
    global thread_stop, number_of_elements

    thread_stop = False
    sequences_thread = threading.Thread(target=write_missing_images, args=(missing_images_file,))
    sequences_thread.daemon = True
    sequences_thread.start()
    
    connections_thread = threading.Thread(target=monitor_connections, args=())
    connections_thread.daemon = True
    connections_thread.start()
    
    chunks  = np.array_split(pd.read_csv(metadata_filepath),max_workers)
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
    for result in as_completed(results):
        pass
    thread_stop = True

    global missing_images
    logging.warning(f"{len(missing_images)} images could not be downloaded")
    
    sequences_thread.join()
    connections_thread.join()
            
    t2 = time.time()
    logging.warning(f"it took {round(t2-t1,2)} seconds to download {len(os.listdir(resized_dir))} images out of {number_of_elements}")
        
"""---------------------------------------------------------------------------------"""
lock = threading.Lock()
missing_images_lock = threading.Lock()
allowed_connection_lock = threading.Lock()
allowed_connections = 3000
allowed_connections_current = allowed_connections
missing_images = []
thread_stop = True
number_of_elements = 0
number_of_elements_lock = threading.Lock()

if __name__ == '__main__':
 
    main_path = os.getcwd()
    main_path = '/mnt/sds-hd/sd17f001/eren/mapillary_final/image_metadata'
    
    metadata_path = os.path.join(main_path,'csv_files')
    missing_images_filepath = os.path.join(main_path,'missing_images')
    
    metadata_file = 'test.csv'
    metadata_file = sys.argv[1]
    missing_images_file = os.path.join(missing_images_filepath,f'missing_{metadata_file}')
    metadata_file = os.path.join(metadata_path,metadata_file)
    
    dir = os.path.join(main_path,metadata_file.split('.')[0])
    
    if not os.path.exists(main_path):
        os.makedirs(main_path, exist_ok=True)
    if not os.path.exists(metadata_path):
        os.makedirs(metadata_path, exist_ok=True)
    if not os.path.exists(dir):
        os.makedirs(dir, exist_ok=True)
    if not os.path.exists(missing_images_filepath):
        os.makedirs(missing_images_filepath, exist_ok=True)

    t1 = time.time()
    
    main(metadata_file, missing_images_file)

    
   
    
