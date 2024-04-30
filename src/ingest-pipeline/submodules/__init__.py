import sys
import os
from importlib import import_module

sys.path.append(os.path.join(os.path.dirname(__file__),
                             'atlas-d2k-py', 'atlas_d2k'))


atlasd2k_prepare_replicate = import_module('pipelines.scRNASeq.prepare_replicate')


__all__ = ["atlasd2k_prepare_replicate",
           ]

sys.path.pop()
