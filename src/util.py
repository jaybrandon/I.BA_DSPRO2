import os
import random

import numpy as np


def set_seed(seed=42):
    """
    Seeds all random number generators to ensure reproducibility.
    """

    random.seed(seed)
    os.environ["PYTHONHASHSEED"] = str(seed)

    np.random.seed(seed)

    print(f"Global seed set to {seed}")