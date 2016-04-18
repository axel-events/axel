# axel
*Axel* is a very small Python library for events handling. 

Examples:

    from axel import Event
    
    event = Event()
    def on_event(*args, **kwargs):
        return (args, kwargs)
    
    event += on_event                     #handler registration
    print(event(10, 20, y=30))        
    >> ((True, ((10, 20), {'y': 30}), <function on_event at 0x00BAA270>),)
    
    event -= on_event                     #handler is unregistered
    print(event(10, 20, y=30))     
    >> None
    
    
    class Mouse(object):
        def __init__(self):
            self.click = Event(self)
            self.click += self.on_click   #handler registration
    
        def on_click(self, sender, *args, **kwargs):
            assert isinstance(sender, Mouse), 'Wrong sender'
            return (args, kwargs)
    
    mouse = Mouse()
    print(mouse.click(10, 20))
    >> ((True, ((10, 20), {}),
    >>  <bound method Mouse.on_click of <__main__.Mouse object at ...>>),)
    
    mouse.click -= mouse.on_click         #handler is unregistered
    print(mouse.click(10, 20))
    >> None
         

Features:

  - handlers can receive the sender as their first argument
  - handlers are always executed in separate threads
  - handlers may be executed synchronous or asynchronous
  - handlers execution may be synchronized on a lock
  - the time allocated for the execution of a handler is controllable
  - the number of actively executed handlers at one time is controllable
  - the execution result of a handler can be memoized 
  - the result of the execution is provided as a tuple ((flag, result, handler),)
  - in case of error, the traceback may be provided for each handler in error

