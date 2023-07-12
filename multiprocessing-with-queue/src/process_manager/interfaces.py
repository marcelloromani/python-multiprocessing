class MsgProducer:
    """Message Producer interface"""

    def yield_msg(self):
        """Yield all messages"""
        raise NotImplementedError()


class MsgConsumer:
    """Message Consumer interface"""

    def process_msg(self, msg):
        """Process a single message"""
        raise NotImplementedError()
