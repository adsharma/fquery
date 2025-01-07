# This exists to export fsqlmodel as sqlmodel
# The rename had to be done to disambiguate and avoid circular deps
import sys

from . import fsqlmodel

sys.modules[__package__ + ".sqlmodel"] = fsqlmodel
