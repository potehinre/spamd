import asyncio
import logging
import json
import aiohttp
from aio_pika import connect_robust


logger = logging.getLogger('spamd.server')


def check_message(msg):
    dct = json.loads(msg)
    if 'owner_id' not in dct:
        raise ValueError("message should contain owner_id key")
    if 'text' not in dct:
        raise ValueError("message should contain text key")
    return {"owner_id": dct["owner_id"], "text": dct["text"],
            "id": dct.get("id", ""), "source": dct.get("source", "")}


async def alert(url, token, batch):
    headers_dict = {"Authorization": "Token {}".format(token),
               "Content-Type": "application/json"}
    async with aiohttp.ClientSession(headers=headers_dict) as session:
        async with session.post(url, json=batch) as resp:
            text = await resp.text()
            if resp.status not in (200, 201):
                logger.error("alert to url {} returned incorrect status code: {} {}".format(
                             url, resp.status, text))
            else:
                logger.info("alerted to url {} about spam with result {} {}".format(
                    url, resp.status, text))


async def serve(loop, spam_filter, connstring, queue_name, batch_size, alert_url, token):
    connection = await connect_robust(connstring=connstring, loop=loop)
    channel = await connection.channel()
    queue = await channel.declare_queue(queue_name, durable=True)

    batch = []
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                logger.info("received a message for filtering: {0}".format(str(message.body)))
                msg = None
                try:
                    msg = check_message(message.body)
                except Exception as e:
                    logger.error("incorrect message format {0}: {1}".format(str(message.body), str(e)))
                    continue
                batch.append(msg)
                if len(batch) >= batch_size:
                    is_spams = spam_filter.is_spam([msg['text'] for msg in batch])
                    for i, is_spam in enumerate(is_spams):
                        if is_spam:
                            logger.info("message is a spam: {0}".format(batch[i]['text']))
                    await alert(alert_url, token, batch)
                    batch = []

                if queue.name in message.body.decode():
                    break


def start(spam_filter, connstring, queue_name, batch_size, alert_url, alert_token):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(serve(loop,
                                  spam_filter, connstring,
                                  queue_name, batch_size,
                                  alert_url, alert_token))
    loop.close()
