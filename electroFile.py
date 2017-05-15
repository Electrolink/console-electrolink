#!/usr/local/bin/python
# -*- coding: utf-8 -*-
import pytoml as toml
import paho.mqtt.client as mqtt
from json import loads, dumps
from termcolor import colored
import sys
import os
import base64
import ntpath
import time
import progressbar
import math

def is_binary(filename):
    """Return true if the given filename is binary.
    @raise EnvironmentError: if the file does not exist or cannot be accessed.
    @attention: found @ http://bytes.com/topic/python/answers/21222-determine-file-type-binary-text on 6/08/2010
    @author: Trent Mick <TrentM@ActiveState.com>
    @author: Jorge Orpinel <jorge@orpinel.com>"""
    fin = open(filename, 'rb')
    try:
        CHUNKSIZE = 1024
        while 1:
            chunk = fin.read(CHUNKSIZE)
            if '\0' in chunk: # found null byte
                return True
            if len(chunk) < CHUNKSIZE:
                break # done
    # A-wooo! Mira, python no necesita el "except:". Achis... Que listo es.
    finally:
        fin.close()

    return False


def getFileSize(name):
    fileSize = os.stat(name)[6] # st_size - made compatible with micropython
    return {"size":fileSize, "unit":"bytes"}

# reads file in chunks then encode them in base64 mode. Generator is returned here
def getFileStream(path):
    file_object = open(path, "r")
    if (is_binary(path)):
        return (base64.b64encode(i) for i in read_in_chunks(file_object, chunkSize))
    else :
        return (i for i in read_in_chunks(file_object, chunkSize))

def read_in_chunks(file_object, chunk_size):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data

def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)

def on_connect(mqttc, obj, flags, rc):
    #t = colored('\nConnected to broker', 'green', attrs=['reverse'])
    print("Connected to broker")

def on_message(mqttc, obj, msg):
    global received
    global msgPart
    global value
    global streamEnd
    global pingReceived
    global msgCounter

    data = loads(str(msg.payload))
    #print(data)

    if ("ping" in data["requested"]):
        pingReceived = True
        print(colored(thingName + " replied", 'green', attrs=['reverse']))

    else :

        if ("msgPart" in data):
            msgPart = int(data["msgPart"])
            if (msgPart == -1):
                streamEnd = False
            else :
                if (get is True): ################################ GET
                    filename = data["params"][0]
                    if (len(sys.argv)>3):
                        filename = sys.argv[3]
                    f = None
                    if (msgPart == 0):
                        f = open(filename, "wb")
                    else :
                        f = open(filename, "ab")

                    if (msgPart == msgCounter):
                        try :
                            decoded = base64.b64decode(data["value"])
                            f.write(decoded)
                        except:
                            f.write(data["value"])
                        print("\b"+str(os.stat(filename)[6]/1000) + " kb" )
                    else :
                        print(colored("Error occured in transfering file", 'red', attrs=['reverse']))
                        exit()
                    msgCounter+=1

        value = data["value"]
        received = True

value = 0
msgCounter = 0
msgPart = 0
received = False
put = False
get = False
ls = False
ping = True # ping at start
cnt = 0
chunkSize = 3696 #1224
streamEnd = True
pingReceived = False
thingName = None

if (len(sys.argv)>1) :
    if (sys.argv[1] == "put"):
        put = True
    elif(sys.argv[1] == "get"):
        get = True
    elif(sys.argv[1] == "ls"):
        ls = True
    elif(sys.argv[1] == "ping"):
        ping = True

    if ((put is True) or (get is True) or (ls is True) or (ping is True)):

        print("Reading configuration from config.toml file")
        config = ""
        with open('config.toml', 'rb') as configFile:
            config = toml.load(configFile)

        thingName = config["thing"]["device_id"]
        server = config["server"]["ip"]
        port = config["server"]["port"]
        keepalive = config["server"]["keepalive"]
        commandTopic = "mainflux/channels/"+config["thing"]["command_topic"]+"/msg"
        replyTopic = "mainflux/channels/"+config["thing"]["reply_topic"]+"/msg"
        errorTopic = "mainflux/channels/"+config["thing"]["error_topic"]+"/msg"

        # If you want to use a specific client id, use
        # mqttc = mqtt.Client("client-id")
        # but note that the client id must be unique on the broker. Leaving the client
        # id parameter empty will generate a random id for you.
        msgId = "electroFile"
        mqttc = mqtt.Client(msgId)
        mqttc.on_message = on_message
        mqttc.on_connect = on_connect
        #mqttc.on_publish = on_publish
        #mqttc.on_subscribe = on_subscribe
        # Uncomment to enable debug messages
        #mqttc.on_log = on_log


        print("Connecting to :" + server + " on port " + str(port))
        mqttc.connect(server, port, keepalive)
        mqttc.subscribe(replyTopic, 0)
        mqttc.subscribe(errorTopic, 0)
        mqttc.loop_start()
        
        if (ping is True):
            p = {"method":"ping", "params":[]}
            p["id"] = msgId
            out = dumps(p)
            mqttc.publish(commandTopic, out)
            while (pingReceived is False):
                time.sleep(0.1)


        if (put is True):

            path = sys.argv[2]
            filename = path_leaf(path)
            if (len(sys.argv)>3):
                filename = sys.argv[3]

            totalMsgParts = math.ceil(float(getFileSize(path)["size"])/chunkSize)

            bar = progressbar.ProgressBar(maxval=totalMsgParts, \
                widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
            bar.start()

            command = "writeBinaryFile"
            if not(is_binary(path)):
                command = "writeTextFile"

            cnt = 0
            for piece in getFileStream(sys.argv[2]):

                outData = piece
                writeFlag = "wb"
                if (cnt>0): # First write is overwrite!!! All others are appending to file
                    writeFlag = "ab"
                p = {"method":command, "params":[filename, outData, writeFlag], "msgPart":cnt}
                if not(msgId is None):
                    p["id"] = msgId
                out = dumps(p)
                mqttc.publish(commandTopic, out)
                while(received is False):
                    pass
                received = False
                if (msgPart == cnt):
                    bar.update(msgPart)
                else :
                    print(colored("Error occured in transfering file", 'red', attrs=['reverse']))
                    exit()
                cnt+=1
            bar.finish()


        if (get is True):

            path = sys.argv[2]
            filename = path_leaf(path)
            if (len(sys.argv)>3):
                filename = sys.argv[3]

            command = "getFile"
            p = {"method":command, "params":[filename]}
            out = dumps(p)
            mqttc.publish(commandTopic, out)
            while (streamEnd is True):
                time.sleep(0.1)
            print(colored("Transfer completed!", 'green', attrs=['reverse']))

        if (ls is True):
            path = "."
            if (len(sys.argv)>2):
                path = sys.argv[2]

            filename = path_leaf(path)
            if (len(sys.argv)>3):
                filename = sys.argv[3]

            command = "getFileList"
            p = {"method":command, "params":[filename]}
            out = dumps(p)
            mqttc.publish(commandTopic, out)
            while(received is False):
                pass
            for i in value:
                t = colored(i, 'green')
                print(t)

    else :
        print(colored("Error! Second parameter must be : put, get, ls or ping", 'red', attrs=['reverse']))

else :
    print(colored("Error! Second parameter must be : put, get, ls or ping", 'red', attrs=['reverse']))

        #mqttc.publish(commandTopic, json.dumps(out))