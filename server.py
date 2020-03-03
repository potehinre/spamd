import asyncio
import logging
from aio_pika import connect_robust


logger = logging.getLogger('spamd.server')


async def serve(loop, spam_filter, connstring, queue_name, batch_size):
    connection = await connect_robust(connstring=connstring, loop=loop)
    channel = await connection.channel()
    queue = await channel.declare_queue(queue_name)

    batch = []
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                logger.info("received a message for filtering: {0}".format(str(message.body)))
                batch.append(str(message.body))
                if len(batch) >= batch_size:
                    is_spams = spam_filter.is_spam(batch)
                    for i, is_spam in enumerate(is_spams):
                        if is_spam:
                            logger.info("message is a spam: {0}".format(batch[i]))
                    batch = []

                if queue.name in message.body.decode():
                    break


def start(spam_filter, connstring, queue_name, batch_size):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(serve(loop,
                                  spam_filter, connstring,
                                  queue_name, batch_size))
    loop.close()
