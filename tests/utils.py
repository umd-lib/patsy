from patsy.model import Accession, Restore, Transfer
from faker import Faker
import random
import os


class AccessionBuilder:
    """
    Builder for an Accession object, initially populated with random values.

    Specific values can be set using the setter methods, i.e.,

    AccessionBuilder().set_filename('foo.txt').build()
    """
    def __init__(self):
        fake = Faker()
        file_path = fake.file_path()

        self.batch = 'Batch' + str(random.randint(0, 100))
        self.bytes = random.randint(0, 10000000)
        self.relpath = file_path[1:]
        self.md5 = fake.md5()
        self.filename = os.path.basename(file_path)
        self.sourcefile = fake.file_path()
        self.sourceline = random.randint(0, 10000)
        self.timestamp = str(fake.date_time())

    def set_md5(self, md5):
        """
        Sets the MD5 checksum
        :param md5: the MD5 checksum to set
        :return: self
        """
        self.md5 = md5
        return self

    def set_filename(self, filename):
        """
        Sets the base filename
        :param filename: the base filename to set
        :return: self
        """
        self.filename = filename
        self.relpath = os.path.dirname(self.relpath) + "/" + filename
        return self

    def set_bytes(self, bytes):
        """
        Sets the number of bytes
        :param bytes: the number of bytes
        :return: self
        """
        self.bytes = bytes
        return self

    def set_batch(self, batch):
        """
        Sets the name of the batch the accession belongs to
        :param batch: the name of the batch
        :return: self
        """
        self.batch = batch
        return self

    def set_relpath(self, relpath):
        """
        Sets the relative path of the accession
        :param relpath: the relative path of the accession
        :return: self
        """
        self.relpath = relpath
        return self

    def build(self):
        """
        Returns an Accession object
        :return: as Accession object
        """
        return Accession(batch=self.batch, bytes=self.bytes, filename=self.filename,
                         md5=self.md5, relpath=self.relpath, sourcefile=self.sourcefile,
                         sourceline=self.sourceline, timestamp=self.timestamp)


class RestoreBuilder:
    """
    Builder for a Restore object, initially populated with random values.

    Specific values can be set using the setter methods, i.e.,

    RestoreBuilder().set_filename('foo.txt').build()
    """
    def __init__(self):
        fake = Faker()

        self.md5 = fake.md5()
        self.filepath = fake.file_path()
        self.filename = os.path.basename(self.filepath)
        self.bytes = random.randint(0, 10000000)

    def set_md5(self, md5):
        """
        Sets the MD5 checksum
        :param md5: the MD5 checksum to set
        :return: self
        """
        self.md5 = md5
        return self

    def set_filename(self, filename):
        """
        Sets the base filename
        :param filename: the base filename to set
        :return: self
        """
        self.filename = filename
        self.filepath = self.filepath + "/" + filename
        return self

    def set_filepath(self, filepath):
        """
        Sets the filepath
        :param filepath: the filepath to set
        :return: self
        """
        self.filepath = filepath
        return self

    def set_bytes(self, bytes):
        """
        Sets the number of bytes
        :param bytes: the number of bytes
        :return: self
        """
        self.bytes = bytes
        return self

    def build(self):
        """
        Returns a Restore object
        :return: a Restore object
        """
        return Restore(md5=self.md5, filepath=self.filepath, filename=self.filename, bytes=self.bytes)


class TransferBuilder:
    """
    Builder for a Transfer object, initially populated with random values.

    Specific values can be set using the setter methods, i.e.,

    TransferBuilder().set_fileoath('foo.txt').build()
    """
    def __init__(self):
        fake = Faker()

        self.filepath = fake.file_path()
        self.storagepath = fake.file_path()

    def set_filepath(self, filepath):
        """
        Sets the filepath
        :param filepath: the filepath to set
        :return: self
        """
        self.filepath = filepath
        return self

    def set_storagepath(self, set_storagepath):
        """
        Sets the set_storagepath
        :param set_storagepath: the storagepath to set
        :return: self
        """
        self.set_storagepath = set_storagepath
        return self

    def build(self):
        """
        Returns a Transfer object
        :return: a Transfer object
        """
        return Transfer(filepath=self.filepath, storagepath=self.storagepath)


def create_perfect_match(accession):
    """
    Returns a Restore object that is a "perfect" match for the given Accession,
    i.e., has matching "md5", "filename", and "bytes" fields

    :param accession: the Accession to create a perfect match for
    :return: a Restore object that is a "perfect" match for the given Accession
    """
    restore_builder = RestoreBuilder()
    restore_builder.set_md5(accession.md5)
    restore_builder.set_bytes(accession.bytes)
    restore_builder.set_filename(accession.filename)
    restore = restore_builder.build()

    return restore
