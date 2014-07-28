"""
Module to hold a reference to singleton Hubstorage client and Job instance
"""
import os


class _HubstorageRef(object):

    def __init__(self):
        self.enabled = 'SHUB_JOBKEY' in os.environ
        self._client = None
        self._project = None
        self._job = None
        if self.enabled:
            self.jobkey = os.environ['SHUB_JOBKEY']
            self._projectid, self._spiderid, self._jobcounter = \
                map(int, self.jobkey.split('/'))
        else:
            self._projectid = None
            self._spiderid = None
            self._jobcounter = None

    @property
    def auth(self):
        return os.environ['SHUB_JOBAUTH'].decode('hex')

    @property
    def endpoint(self):
        return os.environ.get('SHUB_STORAGE')

    @property
    def projectid(self):
        return self._projectid

    @property
    def spiderid(self):
        return self._spiderid

    @property
    def jobid(self):
        return self._jobcounter

    @property
    def client(self):
        from hubstorage.client import HubstorageClient
        if self._client is None:
            self._client = HubstorageClient(endpoint=self.endpoint,
                                            auth=self.auth)
        return self._client

    @property
    def project(self):
        if self._project is None:
            self._project = self.client.get_project(str(self.projectid))
        return self._project

    @property
    def job(self):
        if self._job is None:
            self._job = self.project.get_job((self.spiderid, self.jobid))
        return self._job

    def close(self):
        if self._client is not None:
            self._client.close()

hsref = _HubstorageRef()
