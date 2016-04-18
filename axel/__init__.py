# __init__.py
#
# Copyright (C) 2016 Adrian Cristea adrian dot cristea at gmail dotcom
#
# This module is part of axel and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php 

import inspect
from .axel import *
__all__ = sorted(name for name, obj in locals().items()
                 if not (name.startswith('_') or inspect.ismodule(obj))) 
__all__.append('axel')
del inspect
