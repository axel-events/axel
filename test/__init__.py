import sys, axel, unittest, time, threading
from time import sleep
try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty

try:
    threading.stack_size(64*1024)
except:
    pass
        
wait = 0

class EventTester(object):
    def __init__(self):
        self.event = axel.Event(self)
        self.wait = 0
    
    def event_listener(self, sender, *args, **kw):
        assert isinstance(sender, EventTester)
        if self.wait:
            time.sleep(self.wait)
        return (args, kw)
        
def event_listener(sender, *args, **kw):
    assert isinstance(sender, EventTester)
    if wait:
        time.sleep(wait)
    return (args, kw)
    
   
class TestAxel(unittest.TestCase):    
    def setUp(self):
        self.et = EventTester()
        self.et.event += self.et.event_listener
        self.et.event += event_listener

    def test_default(self):
        result = self.et.event(10, 20, p3=30)
        for r in result:
            self.assertEqual(r[0], True)
            self.assertEqual(r[1], ((10, 20), {'p3':30}))

    def test_same_thread(self):
        self.et.event.threads = 0        
        result = self.et.event(10, 20)
        for r in result:
            self.assertEqual(r[0], True)
            self.assertEqual(r[1], ((10,20),{}))
             
    def test_register_unregister(self):
        key = hash(event_listener)
        self.assertTrue(key in self.et.event.handlers)
        self.et.event -= event_listener
        self.assertTrue(key not in self.et.event.handlers)
 
    def test_asynch(self):
        self.et.event.threads = 1
        self.et.event.asynch = True
        
        result = self.et.event(10, 20)
        for r in result:
            self.assertEqual(r[0], None)
            self.assertEqual(r[1], None)
 
    def test_exc_info(self):
        self.et.event.exc_info = True
        self.et.event.traceback = True
         
        def on_event_error(sender, x, y):
            raise ValueError('Dummy error')
         
        self.et.event.clear()
        self.et.event += on_event_error
         
        r = self.et.event(10, 20)[0]        
        self.assertTrue(isinstance(r[1][1], ValueError))
        self.assertEqual(str(r[1][1]), 'Dummy error')
                 
    def test_memoize(self):
        self.et.event += (event_listener, True)
        self.et.event(10, 20)
         
        key = hash(event_listener)  
        self.assertTrue(key in self.et.event.memoize)
         
    def test_timeout(self):
        global wait 
        wait = .5
        self.et.event.clear()
        self.et.event += (event_listener, True, .2)
         
        err = self.et.event(10, 20)[0][1]
        self.assertTrue(isinstance(err, RuntimeError))
 

             
class TestRapidEventQueueing(unittest.TestCase):
    """
    Test for investigating weird behaviour in rapid calls to events that are being queued.
 
    Contributed by Rob van der Most <Rob@rmsoft.nl>    
     
    Have an event to which a single method is registered. This method will put the arguments
    from the call on a queue. Call the event several times in rapid succession. Check that no
    weird behaviour occurs.
    """
    def setUp(self):
        self.queue = Queue()
        self.event_class = axel.Event
 
    def event_listener(self, *args, **kwargs):
        self.queue.put_nowait((args, kwargs))
 
    def test_rapid_calls(self):
        the_event = self.event_class()
        the_event += self.event_listener
 
        for value in range(300):
            the_event(value)  
        last_value = -1
        try:
            while True:
                next_value = self.queue.get_nowait()[0][0]
                self.assertEqual(next_value, last_value+1)
                last_value = next_value
 
        except Empty:
            pass
 
        self.assertEqual(last_value, 299)
         
 
    def test_event_raise_blocks_until_ready(self):
        """
        Expect the call to the event will not return until all listeners are called.
        """
        the_event = self.event_class()
        the_event += self.event_listener
 
        for value in range(300):
            the_event(value)
            stored_value = self.queue.get_nowait()[0][0]
            self.assertEqual(value, stored_value)
                  
if __name__=='__main__':
    unittest.main()