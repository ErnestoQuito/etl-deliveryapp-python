def get_setting(filename: str, key: str):
    import configparser
    import os
    
    ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    PATH_FILE = os.path.join(ROOT, 'configuracion', filename)
    config = configparser.ConfigParser()
    config.read(PATH_FILE)
    return config._sections[key]
