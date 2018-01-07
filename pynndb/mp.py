from multiprocessing.managers import BaseManager, Server, State, process
from multiprocessing.context import ProcessError
import threading
import sys

class LocalServer(Server):

    def serve_forever(self, context):
        '''
        Run the server forever
        '''
        self.stop_event = threading.Event()
        process.current_process()._manager_server = self
        try:
            accepter = threading.Thread(target=self.accepter)
            accepter.daemon = True
            accepter.start()
            try:
                while not self.stop_event.is_set():
                    self.stop_event.wait(1)
            except (KeyboardInterrupt, SystemExit):
                print()
        finally:
            if sys.stdout != sys.__stdout__:  # what about stderr?
                print('resetting stdout, stderr')
                sys.stdout = sys.__stdout__
                sys.stderr = sys.__stderr__


class LocalManager(BaseManager):

    def get_server(self):
        '''
        Return server object with serve_forever() method and address attribute
        '''
        if self._state.value != State.INITIAL:
            if self._state.value == State.STARTED:
                raise ProcessError("Already started server")
            elif self._state.value == State.SHUTDOWN:
                raise ProcessError("Manager has shut down")
            else:
                raise ProcessError(
                    "Unknown state {!r}".format(self._state.value))
        return LocalServer(self._registry, self._address,
                      self._authkey, self._serializer)