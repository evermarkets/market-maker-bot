import abc


class strategy_interface(abc.ABC):

    @abc.abstractmethod
    async def run(self):
        pass

    @abc.abstractmethod
    async def handle_exception(self, err_msg):
        pass
