class Worker:

    def __init__(self, msg_processing_function):
        """
        :param msg_processing_function: function that processes the message
        """
        self._msg_processor = msg_processing_function

    def process_msg(self, msg):
        self._msg_processor(msg)
