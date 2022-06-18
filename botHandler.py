import asyncio
from telethon import TelegramClient, events, sync
from config import api_id , api_hash


client = TelegramClient('bot', api_id, api_hash)

def sendMessage(msg):
    client.start()
    client.send_message('gettrendsignal' , message=msg)



