# -*- coding: utf-8 -*-

"""
fuzzydata.core.artifact
~~~~~~~~~~~~
This module contains the Artifact abstract base class definition for clients to implement
:copyright: (c) Suhail Rehman 2022
:license: MIT, see LICENSE for more details.
"""

from abc import abstractmethod, ABC

import pandas as pd
import logging

logger = logging.getLogger(__name__)


class Artifact(ABC):
    """
    Generic Artifact representation
    """
    def __init__(self, label, schema_map=None, filename=None, file_format='csv',
                 in_memory=False):
        """ Artifact Instantiation Method
        :param label: The label to provide for this artifact
        :param schema_map: Mapping of column_name: faker_provider for this artifact
        :param filename: Path to filename to be used for serialization
        :param file_format: File format to be used for this artifact serialization (default CSV)
        :param in_memory: Flag if the artifact is in memory or not
        """
        self.filename = filename
        self.label = label
        self.in_memory = in_memory
        self.file_format = file_format
        self.schema_map = schema_map

        logger.debug(f'New Artifact: {label}')

    @abstractmethod
    def generate(self, num_rows, schema):
        """ Abstract method which invokes generate_table function and stores it somehow
        :param num_rows: Number of rows to be generated
        :param schema: Mapping of column_name: faker_provider for this artifact
        """

    @abstractmethod
    def from_df(self, df):
        """ Abstract method which accepts a dataframe as input and stores inside this artifact object
        :param df: Dataframe from which this artifact has to be instantiated.
        """

    @abstractmethod
    def deserialize(self, filename):
        """ Abstract method to load artifact from disk using some serialization method
        :param filename: Filename to be written out to
        """

    @abstractmethod
    def serialize(self, filename):
        """ Abstract method to store artifact to disk using some serialization method
        :param filename: Filename to be read from
        """

    @abstractmethod
    def destroy(self):
        """ Destructor when this artifact needs to deleted from memory"""

    @abstractmethod
    def to_df(self) -> pd.DataFrame:
        """ Return a dataframe representation of this artifact
        :return Dataframe representation of this artifact.
        """

    def __len__(self):
        """ Abstract representation: should return the number of rows in this artifact"""

    def __repr__(self):
        return f"Artifact(label={self.label})"
