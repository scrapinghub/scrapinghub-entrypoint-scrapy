"""
Module to hold a reference to singleton Hubstorage client and Job instance
"""
import os
from codecs import decode
from sh_scrapy.compat import to_native_str


class _HubstorageRef(object):

    def __init__(self):
        self.enabled = 'SHUB_JOBKEY' in os.environ
        self._client = None
        self._project = None
        self._job = None
        if self.enabled:
            self.jobkey = os.environ['SHUB_JOBKEY']
            job_id = [int(id) for id in self.jobkey.split('/')]
            self._projectid, self._spiderid, self._jobcounter = job_id
        else:
            self._projectid = None
            self._spiderid = None
            self._jobcounter = None

    @property
    def auth(self):
        return to_native_str(decode(os.environ['SHUB_JOBAUTH'], 'hex'))

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
        from scrapinghub import HubstorageClient
        if self._client is None:
            user_agent = os.environ.get('SHUB_HS_USER_AGENT')
            self._client = HubstorageClient(endpoint=self.endpoint,
                                            auth=self.auth,
                                            user_agent=user_agent)
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
