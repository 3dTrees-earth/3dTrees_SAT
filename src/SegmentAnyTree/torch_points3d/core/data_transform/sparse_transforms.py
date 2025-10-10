import torch
import random


class RandomCoordsFlip(object):
    def __init__(self, ignored_axis, is_temporal=False, p=0.95):
        """This transform is used to flip sparse coords using a given axis. Usually, it would be x or y

        Parameters
        ----------
        ignored_axis: str
            Axis to be chosen between x, y, z
        is_temporal : bool
            Used to indicate if the pointcloud is actually 4 dimensional

        Returns
        -------
        data: Data
            Returns the same data object with only one point per voxel
        """
        assert 0 <= p <= 1, (
            "p should be within 0 and 1. Higher probability reduce chance of flipping"
        )
        self._is_temporal = is_temporal
        self._D = 4 if is_temporal else 3
        mapping = {"x": 0, "y": 1, "z": 2}
        self._ignored_axis = [mapping[axis] for axis in ignored_axis]
        # Use the rest of axes for flipping.
        self._horz_axes = set(range(self._D)) - set(self._ignored_axis)
        self._p = p

    def __call__(self, data):
        for curr_ax in self._horz_axes:
            if random.random() < self._p:
                coords = data.coords
                coord_max = torch.max(coords[:, curr_ax])
                data.coords[:, curr_ax] = coord_max - coords[:, curr_ax]
        return data

    def __repr__(self):
        return "{}(flip_axis={}, prob={}, is_temporal={})".format(
            self.__class__.__name__, self._horz_axes, self._p, self._is_temporal
        )
