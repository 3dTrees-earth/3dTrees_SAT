import logging

log = logging.getLogger(__name__)

try:
    pass
except:
    log.warning(
        "Sparse convolutions are not supported, please install one of the available backends, MinkowskiEngine or MIT SparseConv"
    )

try:
    pass
except:
    log.warning("MinkowskiEngine is not installed.")
