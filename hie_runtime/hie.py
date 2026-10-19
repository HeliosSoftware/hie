"""Helpers available to transform scripts (`from hie import DataError`)."""


class DataError(Exception):
    """The source record is wrong. HIE rejects it with this message instead of retrying."""
