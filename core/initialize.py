import json
def get_settings():
    f = open("result.txt", "w+")
    f.close()
    with open('./settings.json') as file:
        settings = json.load(file)
    if (any(x not in settings for x in ['DATAPATH','FILENAME','DATABASE_NAME','LIBRARIES','QUERIES','NUM_OF_TESTS'])):
        print("settings have wrong format. Please include DATAPATH,FILENAME,DATABASE_NAME and LIBRARIES as array")
        exit(0)
    return settings