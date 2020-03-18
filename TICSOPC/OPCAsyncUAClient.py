import asyncio, redis, json, sys, signal, csv
from TICSUtil import *
from asyncua import Client, Node, ua
from asyncua.ua import VariantType, Variant, AttributeIds, DataValue

#Initialize tag dictionaries.
rclient = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses = True)

class SubHandler(object):
    """ Client to subscription. It will receive data and events from server."""
    def datachange_notification(self, node, val, data):
        try:
            key = node.nodeid.to_string()
            rclient.hset('tagread',key,str(val))
            tag_name = rclient.hget('tagdb_addr',key)
            is_evt_tag = rclient.hexists('events', tag_name)
            is_first_update = not rclient.hexists('first_update', tag_name)    
            #ignore first updates. These are received when subscribed.
            #Add tag to 'first_update' hash to mark it as already update received.
            if(is_first_update):
                rclient.hset('first_update',tag_name,str(val))   

            if (is_evt_tag):
                if(not is_first_update):
                    evt_data = {'tag':tag_name, 'value':val, 'data':'test data'}
                    rclient.publish('event_queue', str(evt_data))

            # print(log_time(), "Python: New data change event", node.nodeid.to_string(), val)
        except Exception as e:
            print(log_time(), "<ERR> SubhandlerErr: ", e)

    def event_notification(self, event):
        # print(log_time(), "Python: New event", event)
        pass

class TicsOPCAysncUA:

    def __init__(self, tagFileName='tags.csv', heartBeatTag='opc_heartbeat'):
        self.is_connected = False
        self.prevHB = 0
        self.unchanged = 0
        self.first_connect_pending = True
        self.heartBeatTag = heartBeatTag
        self.tagFilename = tagFileName
        self.stop = False

    async def read_tag_file(self, filename):
        """Read tags from a csv file and store them in redis hash called tagdb."""
        print(log_time(), f'<INFO>read_tag_file')
        try:
            root = ".\TICSOPC"
            filename = os.path.join(root,filename)
            with open(filename) as tags_file:
                csv_reader = csv.DictReader(tags_file, delimiter=',')
                line_count = 0
                rclient.delete('tagdb')
                for row in csv_reader:
                    commented_line = row['tag_name'].startswith('#')
                    if ( not commented_line ):
                        tagname = row['tag_name']
                        tagaddr = row['tag_address']
                        rclient.hset('tagdb', tagname, tagaddr)
                        rclient.hset('tagdb_addr', tagaddr, tagname)
                    line_count+=1
                print(log_time(), f'<INFO> Processed {line_count} lines from {filename}.')

        except Exception as e:
            print(log_time(), "<ERR> CSV Read Error: ", filename, "Error:", e)

    async def writetagvalue(self, tag, value):
        """write a single tag value. Dynamically get datatype of tag. For some types (like Int64) it is not required to get type.
            It maybe faster to to not get datatype"""
        try:
            if (self.is_connected):
                node = self.client.get_node(tag)
                valwithtype = Variant(value, await node.get_data_type_as_variant_type())
                await node.set_attribute(AttributeIds.Value, DataValue(valwithtype))
        except Exception as e:
            self.is_connected = False
            print(log_time(), "<ERR> Write Value Exception for tag ", tag, "Error:", e)

    async def write_tags(self):
        print(log_time(), f'<INFO> write_tags')
        while (self.is_connected or self.first_connect_pending):
            try:
                tags = rclient.hgetall('tagwrite')
                for tag, val in tags.items():
                    await self.writetagvalue(tag, json.loads(val))
                    rclient.hdel('tagwrite', tag)
            except Exception as e:
                rclient.hdel('tagwrite', tag)
                self.is_connected = False
                print(log_time(), "<ERR> Write Tags Exception for tag ", tag, "Error:", e)

            try:
                tagsStr = rclient.hgetall('tagwritestr')
                for tag, val in tagsStr.items():
                    await self.writetagvalue(tag, val)
                    rclient.hdel('tagwritestr', tag)
            except Exception as e:
                rclient.hdel('tagwritestr', tag)
                self.is_connected = False
                print(log_time(), "<ERR> Write TagsStr Exception: ", e)

            if (self.stop):
                break
            else:
                await asyncio.sleep(0.1)       #Keep loop running forever

    async def heartbeat(self):
        print(log_time(), f'<INFO> heartbeat')
        self.heartBeatTagAddr = rclient.hget('tagdb', self.heartBeatTag)
        while (self.is_connected or self.first_connect_pending):
            try:
                curHB = rclient.hget('tagread', self.heartBeatTagAddr)
                if (curHB is not None):
                    curHB = json.loads(curHB)
                else:
                    print(self.heartBeatTagAddr)
                    print(log_time(), "<WARN> Heart Beat tag value not found")

                if (self.prevHB ==  curHB):
                    self.unchanged += 1
                else:
                    self.prevHB = curHB
                    self.unchanged = 0

                if (self.unchanged >= 5):
                    print(log_time(), "<ERR> Heart Beat Not Received. Setting Connected status to False")
                    self.is_connected = False               #Set connected status to false and wait for subscribe loop to end
                    self.first_connect_pending = False
                    self.unchanged = 0

            except Exception as e:
                self.is_connected = False
                print(log_time(), "<ERR> Heartbeat: ", e)

            if (self.stop):
                break
            else:            
                await asyncio.sleep(1) 

    async def connectserver(self, url, sub_period):
        print(log_time(), f'<INFO> connectserver')
        await self.read_tag_file(self.tagFilename)
        if (not self.is_connected):
            print('OPC url:' + url)
            self.client = Client(url=url)
            self.handler = SubHandler()
            rclient.delete('first_update')
            rclient.delete('tagread')
            rclient.delete('tagwrite')
            try:
                async with self.client:
                    self.is_connected = True
                    self.sub = await self.client.create_subscription(sub_period, self.handler)

                    tags = rclient.hgetall('tagdb')
                    for tag in tags:
                        tagaddr = rclient.hget('tagdb', tag)
                        tagsub = self.client.get_node(tagaddr)
                        await  self.sub.subscribe_data_change(tagsub)
                        print(log_time(), f'<INFO> {tag} added to subscription')

                    self.first_connect_pending = False
                    # time.sleep(3)
                    while (self.is_connected):
                        if (self.stop):
                            del self.sub
                            del self.client
                            del self.handler
                            break
                        else:
                            await asyncio.sleep(2)   #Wait for very long time. Just to keep subscription alive.

            except ConnectionRefusedError as CnErr:
                self.is_connected = False
                print(log_time(), "<ERR> ConnectServer:", str(CnErr))
            except Exception as e:
                self.is_connected = False
                print(log_time(), "<ERR> An Unknown error occurred in ConnectServer: ", e)
            finally:
                del self.client
                del self.handler

async def debugconsole(sub_period):
    print(log_time(), f'<INFO> debugconsole')
    tag1 = 'ns=2;s=SimChannel.Device1.Tag1'
    tag2 = 'ns=2;s=SimChannel.Device1.Tag2'
    while True:
        # tagread = rclient.hgetall('tagread')
        # tagwrite = rclient.hgetall('tagwrite')
        # print('tagvalues: ', tagread)
        # print('tagwrites: ', tagwrite)
        val = rclient.hget('tagread', tag1)
        if (val is not None):
            val = json.loads(val)
            rclient.hset('tagwrite', tag2, val)

        await asyncio.sleep(sub_period/1000)




#Below code to run this module independently.
async def main():

    def signal_handler(sig, frame):
        print(log_time(), f'<INFO> Stop Request received from User')
        OPCServer.stop = True

    signal.signal(signal.SIGINT, signal_handler)

    debug = 0
    config_file = 'OPCConfig.ini'
    i = 0
    for arg in sys.argv:
        if (arg.lower() == 'debug'):
            debug = 1
        if (arg.lower() == '-c'):
            config_file = sys.argv[i+1]
        i+=1

    print(log_time(), f"<INFO> Reading Config File: ", config_file)
    url = readconfigfile(config_file, 'OPC_Server', 'url')
    sub_period = readconfigfile(config_file, 'OPC_Server', 'subscription_period')
    sub_period = float(sub_period)
    heartbeat_tag = readconfigfile(config_file, 'OPC_Server', 'heartbeat_tag')
    tag_file = readconfigfile(config_file, 'OPC_Server', 'tag_file')

    while True:
        print(log_time(), f"<INFO> Starting Client loop...")
        OPCServer = TicsOPCAysncUA(tagFileName=tag_file, heartBeatTag=heartbeat_tag)
    
        taskRead = loop.create_task(OPCServer.connectserver(url, sub_period))
        taskWrite = asyncio.create_task(OPCServer.write_tags())
        taskHB = loop.create_task(OPCServer.heartbeat())
        if (debug):
            taskDebug = asyncio.create_task(debugconsole(sub_period))
        
        await asyncio.wait([taskRead, taskHB, taskWrite])

        if (OPCServer.stop):
            sys.exit(0)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
