import sys
import os
from importlib import import_module

sys.path.append(os.path.join(os.path.dirname(__file__),
                             "atlas-d2k-py"))


atlasd2k_prepare_replicate = import_module("atlas_d2k.pipelines.scRNASeq.prepare_replicate")
atlas_d2k = import_module("atlas_d2k")


__all__ = ["atlasd2k_prepare_replicate",
           "atlas_d2k",
           ]

sys.path.pop()
