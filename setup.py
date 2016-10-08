# setup.py
#
# Copyright (C) 2016 Adrian Cristea adrian dot cristea at gmail dotcom
#
# This module is part of axel and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php 

from setuptools import setup, find_packages

version = '0.0.6'

setup(name='axel',
      version=version,
      description="Python events handling, C# style",
      long_description="""\
*Axel* is a very small Python library inspired by C#  
events that allows an easy and elegant events handling. 

Examples::

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
 
Features::

  - handlers can receive the sender as their first argument
  - handlers can be executed in the same or separate threads
  - handlers may be executed synchronous or asynchronous
  - handlers execution may be synchronized on a lock
  - the time allocated for the execution of a handler is controllable
  - the number of actively executed handlers at one time is controllable
  - the execution result of a handler can be cached 
  - the result of the execution is provided as a tuple ((flag, result, handler),)
  - in case of error, the traceback may be provided for each handler in error

Contributors::

  Erwin Mayer <traderwin at gmail dot com>
  Rob van der Most <Rob at rmsoft dot nl>

Please see the documentation: https://pythonhosted.org/axel
        """,
      classifiers=[
      'Development Status :: 4 - Beta',
      'Intended Audience :: Developers',
      'License :: OSI Approved :: MIT License',      
      'Operating System :: OS Independent',
      'Programming Language :: Python :: 2',
      'Programming Language :: Python :: 3',      
      'Topic :: Software Development :: Libraries :: Python Modules',      
      'Topic :: Utilities',            
      ],
      keywords='event raise handle',
      author='Adrian Cristea',
      author_email='adrian.cristea@gmail.com ',
      license='MIT',
      url='https://github.com/axel-events/axel',
      # download_url='https://github.com/axel-events/axel/tarball/0.1',
      packages=find_packages('.', exclude=['doc*', 'example*', 'test*']),
      zip_safe=False,
) 
