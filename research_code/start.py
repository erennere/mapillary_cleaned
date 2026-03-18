import yaml
def load_config(filepath='./config.yaml'):
    with open(filepath, 'r') as f:
        cfg = yaml.safe_load(f)
    data_dir = cfg['paths']['data_dir']
    
    def format_path_processed_dir(value):
        if isinstance(value, str):
            return value.format(data_dir=data_dir)
        return value
    
    processed_dir = format_path_processed_dir(cfg['paths']['processed_dir'])
    starter_dir = format_path_processed_dir(cfg['paths']['starter_dir'])

    def format_path(value):
        if isinstance(value, str):
            return value.format(data_dir=data_dir,
                                 processed_dir=processed_dir,
                                 starter_dir=starter_dir)
        return value
    
    for k, v in cfg['paths'].items():
        cfg['paths'][k] = format_path(v)
    for k, v in cfg['filenames'].items():
        cfg['filenames'][k] = format_path(v)
    return cfg